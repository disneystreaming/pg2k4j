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

 *******************************************************************************/

package com.disney.pg2k4j;

import com.amazonaws.services.kinesis.producer.*;
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
import java.util.stream.Stream;

public class SlotReaderKinesisWriter {

    private static final Logger logger = LoggerFactory.getLogger(SlotReaderKinesisWriter.class);
    protected static final ObjectMapper objectMapper = new ObjectMapper();
    private static final String recoveryModeSqlState = "57P03";

    protected final PostgresConfiguration postgresConfiguration;
    protected final ReplicationConfiguration replicationConfiguration;
    protected final KinesisProducerConfiguration kinesisProducerConfiguration;
    protected final String streamName;
    protected long lastFlushedTime;

    public SlotReaderKinesisWriter(PostgresConfiguration postgresConfiguration, ReplicationConfiguration replicationConfiguration,
                                   KinesisProducerConfigurationFactory kinesisProducerConfigurationFactory, String streamName) {
        this.postgresConfiguration = postgresConfiguration;
        this.replicationConfiguration = replicationConfiguration;
        this.kinesisProducerConfiguration = kinesisProducerConfigurationFactory.getKinesisProducerConfiguration();
        this.streamName = streamName;
    }

    /**
     * Runs {@link #readSlotWriteToKinesis()} continuously in a loop.
     */
    public void runLoop()  {
        while (true) {
            readSlotWriteToKinesis();
        }
    }

    /**
     * Initializes a KinesisProducer
     * Initializes a PostgresConnector
     *
     * In a loop, call {@link #readSlotWriteToKinesisHelper(KinesisProducer, PostgresConnector)} until the helper
     * method throws an exception. In which case, exit from the method and log the error.
     */

    void readSlotWriteToKinesis() {
        KinesisProducer kinesisProducer = null;
        try (PostgresConnector postgresConnector = createPostgresConnector(postgresConfiguration, replicationConfiguration)) {
            resetIdleCounter();
            kinesisProducer = createKinesisProducer(kinesisProducerConfiguration);
            logger.info("Consuming from slot {}", replicationConfiguration.getSlotName());
            while (true) {
                readSlotWriteToKinesisHelper(kinesisProducer, postgresConnector);
            }
        }
        catch (SQLException sqlException) {
            logger.error("Received the following error pertaining to the replication stream, reattempting...", sqlException);
            if (sqlException.getSQLState().equals(recoveryModeSqlState)) {
                logger.info("Sleeping for five seconds");
                try {
                    Thread.sleep(5000);
                }
                catch (InterruptedException ie ){
                    logger.error("Interrupted while sleeping", ie);
                }
            }
        }
        catch(IOException ioException) {
            logger.error("Received an IO Exception while processing the replication stream, reattempting...", ioException);
        }
        catch(Exception e) {
            logger.error("Received exception of type {}", e.getClass().toString(), e);
        }
        finally {
            if (kinesisProducer != null) {
                try {
                    kinesisProducer.flushSync();
                }
                catch(Exception e){
                    logger.error("Received exception when trying to flush the producer", e);
                }
                try {
                    kinesisProducer.destroy();
                }
                catch(Exception e){
                    logger.error("Received exception when trying to destroy the producer", e);
                }
            }
        }
    }

    /**
     * Using the PostgresConnector, reads a message from the WAL log.
     *
     * If there is data to be read from the WAL, call {@link #processByteBuffer(ByteBuffer, KinesisProducer, PostgresConnector)}
     * with this data.
     *
     * If there is no data to be read from the WAL, check to see if this object has not flushed records to kinesis for an amount of time
     * exceeding `replicationConfiguration.getUpdateIdleSlotInterval`. If this is the case then, get the latest LSN, and fast
     * forward the stream lsn to this value. Before doing this, make sure we read the remaining data flushed to the stream.
     * @param kinesisProducer {@link KinesisProducer}
     * @param postgresConnector {@link PostgresConnector}
     * @throws SQLException
     * @throws IOException
     */
    void readSlotWriteToKinesisHelper(final KinesisProducer kinesisProducer, final PostgresConnector postgresConnector) throws SQLException, IOException {
        ByteBuffer msg = postgresConnector.readPending();
        if (msg != null) {
            processByteBuffer(msg, kinesisProducer, postgresConnector);
        }
        else if (System.currentTimeMillis() - lastFlushedTime > replicationConfiguration.getUpdateIdleSlotInterval() * 1000) {
            LogSequenceNumber lsn = postgresConnector.getCurrentLSN();
            msg = postgresConnector.readPending();
            if (msg != null) {
                processByteBuffer(msg, kinesisProducer, postgresConnector);
            }
            logger.info("Fast forwarding stream lsn to {} due to stream inactivity", lsn.toString());
            postgresConnector.setStreamLsn(lsn);
            resetIdleCounter();
        }
    }

    /**
     * Parse this message and call {@link #getSlotMessage(byte[], int)} to get the bean representation of this WAL chunk.
     * Pass this off to {@link #getUserRecords(SlotMessage)} )} to get the java stream of UserRecords to then put on the Kinesis Stream.
     * Register the callback defined in {@link #getCallback(PostgresConnector, UserRecord)} to be invoked when
     * the records succeed or fail to be placed on the stream by the prodcuer.
     * @param msg Data coming off the WAL which will act as UserRecord seed
     * @param kinesisProducer {@link KinesisProducer}
     * @param postgresConnector {@link PostgresConnector}
     * @throws IOException
     */
    void processByteBuffer(final ByteBuffer msg, final KinesisProducer kinesisProducer, final PostgresConnector postgresConnector) throws IOException {
        logger.debug("Processing chunk from wal");
        int offset = msg.arrayOffset();
        byte[] source = msg.array();
        SlotMessage slotMessage = getSlotMessage(source, offset);
        if (slotMessage.getChange().size() > 0) {
            getUserRecords(slotMessage).forEach(
                    userRecord -> {
                        if (logger.isTraceEnabled()) {
                            logger.trace("Writing record with data {} to stream", new String(userRecord.getData().array()));
                        }
                        ListenableFuture<UserRecordResult> f = kinesisProducer.addUserRecord(userRecord);
                        final FutureCallback<UserRecordResult> callback = getCallback(postgresConnector, userRecord);
                        Futures.addCallback(f, callback);
                    }
            );
        }
    }

    public void resetIdleCounter() {
        lastFlushedTime = System.currentTimeMillis();
    }

    Stream<UserRecord> getUserRecords(SlotMessage slotMessage) throws JsonProcessingException {
        Stream<ByteBuffer> byteBuffers = Stream.of(ByteBuffer.wrap(objectMapper.writeValueAsBytes(slotMessage)));
        return byteBuffers.map(
                byteBuffer -> {
                    Random r = new Random();
                    return new UserRecord(streamName, Long.toString(System.currentTimeMillis()),
                            new BigInteger(128, r).toString(10), byteBuffer);
                }
        );
    }

    FutureCallback<UserRecordResult> getCallback(PostgresConnector postgresConnector, UserRecord userRecord) {
        return new SlotReaderCallback(this, postgresConnector, userRecord);
    }

    SlotMessage getSlotMessage(byte[] walChunk, int offset) throws IOException {
        SlotMessage slotMessage = objectMapper.readValue(walChunk, offset, walChunk.length, SlotMessage.class);
        Set<String> relevantTables = replicationConfiguration.getRelevantTables();
        if (relevantTables != null) {
            slotMessage.getChange().removeIf(change -> !relevantTables.contains(change.getTable()));
        }
        return slotMessage;
    }

    PostgresConnector createPostgresConnector(PostgresConfiguration postgresConfiguration, ReplicationConfiguration replicationConfiguration) throws SQLException {
        return new PostgresConnector(postgresConfiguration, replicationConfiguration);
    }

    KinesisProducer createKinesisProducer(KinesisProducerConfiguration kinesisProducerConfiguration) {
        return new KinesisProducer(kinesisProducerConfiguration);
    }
}

