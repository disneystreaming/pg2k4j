/*******************************************************************************
 Copyright 2018 Disney Streaming Services

 Licensed under the Apache License, Version 2.0 (the "Apache License")
 with the following modification; you may not use this file except in
 compliance with the Apache License and the following modification to it:
 Section 6. Trademarks. is deleted and replaced with:

 6. Trademarks. This License does not grant permission to use the trade
 names, trademarks, service marks, or product names of the Licensor
 and its affiliates, except as required to comply with Section 4(c) of
 the License and to reproduce the content of the NOTICE file.

 You may obtain a copy of the Apache License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the Apache License with the above modification is
 distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied. See the Apache License for the specific
 language governing permissions and limitations under the Apache License.

 ******************************************************************************/

package com.disney.pg2k4j;

import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecord;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.disney.pg2k4j.models.SlotMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.postgresql.replication.LogSequenceNumber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class SlotReaderKinesisWriter {

    protected static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Logger logger = LoggerFactory.getLogger(
            SlotReaderKinesisWriter.class);
    private static final String recoveryModeSqlState = "57P03";
    private static final int randomBigIntBits = 128;
    private static final int bigIntToStringRadx = 10;
    private static final int recoveryModeSleepMillis = 5000;

    private final PostgresConfiguration postgresConfiguration;
    private final ReplicationConfiguration replicationConfiguration;
    private final KinesisProducerConfiguration kinesisProducerConfiguration;
    private final String streamName;
    private long lastFlushedTime;

    public SlotReaderKinesisWriter(
            final PostgresConfiguration postgresConfigurationInput,
            final ReplicationConfiguration replicationConfigurationInput,
            final KinesisProducerConfigurationFactory
                    kinesisProducerConfigurationFactory,
            final String streamNameInput) {
        this.postgresConfiguration = postgresConfigurationInput;
        this.replicationConfiguration = replicationConfigurationInput;
        this.kinesisProducerConfiguration =
                kinesisProducerConfigurationFactory
                        .getKinesisProducerConfiguration();
        this.streamName = streamNameInput;
    }

    /**
     * Runs {@link #readSlotWriteToKinesis()} continuously in a loop.
     */
    public void runLoop() {
        while (true) {
            readSlotWriteToKinesis();
        }
    }

    public PostgresConfiguration getPostgresConfiguration() {
        return postgresConfiguration;
    }

    public ReplicationConfiguration getReplicationConfiguration() {
        return replicationConfiguration;
    }

    public KinesisProducerConfiguration getKinesisProducerConfiguration() {
        return kinesisProducerConfiguration;
    }

    public String getStreamName() {
        return streamName;
    }

    public long getLastFlushedTime() {
        return lastFlushedTime;
    }

    /**
     * Initializes a KinesisProducer
     * Initializes a PostgresConnector
     * <p>
     * In a loop, call
     * {@link #readSlotWriteToKinesisHelper(KinesisProducer,
     * PostgresConnector)} until the helper
     * method throws an exception. In which case, exit from the method and
     * log the error.
     */

    void readSlotWriteToKinesis() {
        KinesisProducer kinesisProducer = null;
        try (PostgresConnector postgresConnector = createPostgresConnector(
                postgresConfiguration, replicationConfiguration)) {
            resetIdleCounter();
            kinesisProducer = createKinesisProducer(
                    kinesisProducerConfiguration);
            logger.info("Consuming from slot {}", replicationConfiguration
                    .getSlotName());
            while (true) {
                readSlotWriteToKinesisHelper(kinesisProducer,
                        postgresConnector);
            }
        } catch (SQLException sqlException) {
            logger.error("Received the following error pertaining to the "
                    + "replication stream, reattempting...", sqlException);
            if (sqlException.getSQLState().equals(recoveryModeSqlState)) {
                logger.info("Sleeping for five seconds");
                try {
                    Thread.sleep(recoveryModeSleepMillis);
                } catch (InterruptedException ie) {
                    logger.error("Interrupted while sleeping", ie);
                }
            }
        } catch (IOException ioException) {
            logger.error("Received an IO Exception while processing the "
                    + "replication stream, reattempting...", ioException);
        } catch (Exception e) {
            logger.error("Received exception of type {}", e.getClass()
                    .toString(), e);
        } finally {
            if (kinesisProducer != null) {
                try {
                    kinesisProducer.flushSync();
                } catch (Exception e) {
                    logger.error("Received exception when trying to flush the"
                            + " producer", e);
                }
                try {
                    kinesisProducer.destroy();
                } catch (Exception e) {
                    logger.error("Received exception when trying to destroy "
                            + "the producer", e);
                }
            }
        }
    }

    /**
     * Using the PostgresConnector, reads a message from the WAL log.
     * <p>
     * If there is data to be read from the WAL, call
     * {@link #processByteBuffer(ByteBuffer, KinesisProducer,
     * PostgresConnector)}
     * with this data.
     * <p>
     * If there is no data to be read from the WAL, check to see if this
     * object has not flushed records to kinesis for an amount of time
     * exceeding `replicationConfiguration.getUpdateIdleSlotInterval`. If
     * this is the case then, get the latest LSN, and fast
     * forward the stream lsn to this value. Before doing this, make sure we
     * read the remaining data flushed to the stream.
     *
     * @param kinesisProducer   {@link KinesisProducer}
     * @param postgresConnector {@link PostgresConnector}
     * @throws SQLException
     * @throws IOException
     */
    void readSlotWriteToKinesisHelper(final KinesisProducer kinesisProducer,
                                      final PostgresConnector
                                              postgresConnector) throws
            SQLException, IOException {
        ByteBuffer msg = postgresConnector.readPending();
        if (msg != null) {
            processByteBuffer(msg, kinesisProducer, postgresConnector);
        } else if (System.currentTimeMillis() - lastFlushedTime
                > TimeUnit.MILLISECONDS.toSeconds(
                        replicationConfiguration.getUpdateIdleSlotInterval())) {
            LogSequenceNumber lsn = postgresConnector.getCurrentLSN();
            msg = postgresConnector.readPending();
            if (msg != null) {
                processByteBuffer(msg, kinesisProducer, postgresConnector);
            }
            logger.info("Fast forwarding stream lsn to {} due to stream "
                    + "inactivity", lsn.toString());
            postgresConnector.setStreamLsn(lsn);
            resetIdleCounter();
        }
    }

    /**
     * Parse this message and call {@link #getSlotMessage(byte[], int)} to
     * get the bean representation of this WAL chunk.
     * Pass this off to {@link #getUserRecords(SlotMessage)} )} to get the
     * java stream of UserRecords to then put on the Kinesis Stream.
     * Register the callback defined in
     * {@link #getCallback(PostgresConnector, UserRecord)} to be invoked when
     * the records succeed or fail to be placed on the stream by the prodcuer.
     *
     * @param msg               Data coming off the WAL which will act as
     *                          UserRecord seed
     * @param kinesisProducer   {@link KinesisProducer}
     * @param postgresConnector {@link PostgresConnector}
     * @throws IOException
     */
    void processByteBuffer(final ByteBuffer msg, final KinesisProducer
            kinesisProducer, final PostgresConnector postgresConnector)
            throws IOException {
        logger.debug("Processing chunk from wal");
        int offset = msg.arrayOffset();
        byte[] source = msg.array();
        SlotMessage slotMessage = getSlotMessage(source, offset);
        if (slotMessage.getChange().size() > 0) {
            getUserRecords(slotMessage).forEach(
                    userRecord -> {
                        if (logger.isTraceEnabled()) {
                            logger.trace("Writing record with data {} to "
                                    + "stream", new String(userRecord.getData()
                                    .array()));
                        }
                        ListenableFuture<UserRecordResult> f =
                                kinesisProducer.addUserRecord(userRecord);
                        final FutureCallback<UserRecordResult> callback =
                                getCallback(postgresConnector, userRecord);
                        Futures.addCallback(f, callback);
                    }
            );
        }
    }

    public void resetIdleCounter() {
        lastFlushedTime = System.currentTimeMillis();
    }

    Stream<UserRecord> getUserRecords(final SlotMessage slotMessage) throws
            JsonProcessingException {
        Stream<ByteBuffer> byteBuffers = Stream.of(ByteBuffer.wrap(
                objectMapper.writeValueAsBytes(slotMessage)));
        return byteBuffers.map(
                byteBuffer -> {
                    Random r = new Random();
                    return new UserRecord(streamName, Long.toString(System
                            .currentTimeMillis()),
                            new BigInteger(randomBigIntBits, r)
                                    .toString(bigIntToStringRadx), byteBuffer);
                }
        );
    }

    FutureCallback<UserRecordResult> getCallback(final PostgresConnector
                                                         postgresConnector,
                                                 final UserRecord userRecord) {
        return new SlotReaderCallback(this,
                postgresConnector, userRecord);
    }

    SlotMessage getSlotMessage(final byte[] walChunk, final int offset)
            throws IOException {
        SlotMessage slotMessage = objectMapper.readValue(walChunk, offset,
                walChunk.length, SlotMessage.class);
        Set<String> relevantTables = replicationConfiguration
                .getRelevantTables();
        if (relevantTables != null) {
            slotMessage.getChange().removeIf(change -> !relevantTables
                    .contains(change.getTable()));
        }
        return slotMessage;
    }

    PostgresConnector createPostgresConnector(final PostgresConfiguration pc,
                                              final ReplicationConfiguration rc)
            throws SQLException {
        return new PostgresConnector(pc, rc);
    }

    KinesisProducer createKinesisProducer(final KinesisProducerConfiguration
                                                  kpc) {
        return new KinesisProducer(kpc);
    }
}

