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

import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecord;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.disney.pg2k4j.models.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.powermock.reflect.Whitebox;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static junit.framework.TestCase.assertEquals;


public class SlotReaderKinesisWriterTest {

    @Mock
    private SlotReaderKinesisWriter slotReaderKinesisWriter;

    @Mock
    private PostgresConfiguration postgresConfiguration;

    @Mock
    private ReplicationConfiguration replicationConfiguration;

    @Mock
    private KinesisProducerConfigurationFactory kinesisProducerConfigurationFactory;

    @Mock
    private KinesisProducerConfiguration kinesisProducerConfiguration;

    @Mock
    private PostgresConnector postgresConnector;

    @Mock
    private KinesisProducer kinesisProducer;

    private ByteBuffer byteBuffer = ByteBuffer.wrap(testByteArray);

    @Mock
    private UserRecord userRecord;

    @Mock
    private FutureCallback<UserRecordResult> callback;

    @Mock
    private ListenableFuture<UserRecordResult> future;

    @Mock
    private PGReplicationStream pgReplicationStream;

    @Mock
    private ObjectMapper objectMapper;

    @Mock
    private SlotMessage slotMessage;

    @Mock
    private Change change1;

    @Mock
    private Change change2;

    @Mock
    private SQLException sqlException;

    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();

    private LogSequenceNumber lsn = LogSequenceNumber.valueOf(1234);

    private static final int testByteBufferOffset = 0;
    private static final String streamName = "streamName";
    private static final int testIdleSlotRecreationSeconds = 10;
    private static final byte[] testByteArray = "testByteArray".getBytes();
    private static final String correctTableName = "correctTableName";
    private static final String incorrectTableName = "incorrectTableName";
    private static final SlotMessage testSlotMessage = new SlotMessage(123,
            Arrays.asList(new DeleteChange("delete", "testTable", "mySchema",
                    new OldKeys(Arrays.asList("type"), Arrays.asList("value"), Arrays.asList("name")))));

    @Before
    public void setUp() throws Exception {
        Whitebox.setInternalState(slotReaderKinesisWriter, "replicationConfiguration", replicationConfiguration);
        Whitebox.setInternalState(slotReaderKinesisWriter, "postgresConfiguration", postgresConfiguration);
        Whitebox.setInternalState(slotReaderKinesisWriter, "kinesisProducerConfiguration", kinesisProducerConfiguration);
        Whitebox.setInternalState(SlotReaderKinesisWriter.class, "objectMapper", objectMapper);
        Mockito.doReturn(slotMessage).when(objectMapper).readValue(testByteArray, testByteBufferOffset, testByteArray.length, SlotMessage.class);
        Mockito.doReturn(kinesisProducerConfiguration).when(kinesisProducerConfigurationFactory).getKinesisProducerConfiguration();
        Mockito.doReturn(Stream.of(userRecord)).when(slotReaderKinesisWriter).getUserRecords(slotMessage);
        Mockito.doReturn(callback).when(slotReaderKinesisWriter).getCallback(postgresConnector, userRecord);
        Mockito.doReturn(slotMessage).when(slotReaderKinesisWriter).getSlotMessage(testByteArray, testByteBufferOffset);
        Mockito.doReturn(future).when(kinesisProducer).addUserRecord(userRecord);
        Mockito.doReturn(pgReplicationStream).when(postgresConnector).getPgReplicationStream();
        Mockito.doReturn(testIdleSlotRecreationSeconds).when(replicationConfiguration).getUpdateIdleSlotInterval();
        Mockito.doReturn(correctTableName).when(change1).getTable();
        Mockito.doReturn(incorrectTableName).when(change2).getTable();
        Mockito.doCallRealMethod().when(slotMessage).getChange();
        Mockito.doReturn(new HashSet<>(Arrays.asList(correctTableName))).when(replicationConfiguration).getRelevantTables();
        List<Change> changes = new ArrayList<>();
        changes.add(change1);
        changes.add(change2);
        Whitebox.setInternalState(slotMessage, "change", changes);
    }

    @Test
    public void testProcessByteBufferPutsOneToKinesisAddsCallbackPerUserRecord() throws Exception {
        Mockito.doCallRealMethod().when(slotReaderKinesisWriter).processByteBuffer(byteBuffer, kinesisProducer, postgresConnector);
        Mockito.doReturn(ByteBuffer.wrap(testByteArray)).when(userRecord).getData();
        slotReaderKinesisWriter.processByteBuffer(byteBuffer, kinesisProducer, postgresConnector);
        Mockito.verify(slotReaderKinesisWriter, Mockito.times(1)).getCallback(postgresConnector, userRecord);
    }

    @Test
    public void testReadSlotWriteToKinesisHelperCallsProcessByteBufferWhenMsgNotNull() throws Exception {
        Mockito.doCallRealMethod().when(slotReaderKinesisWriter).readSlotWriteToKinesisHelper(kinesisProducer, postgresConnector);
        Mockito.doReturn(byteBuffer).when(postgresConnector).readPending();
        slotReaderKinesisWriter.readSlotWriteToKinesisHelper(kinesisProducer, postgresConnector);
        Mockito.verify(slotReaderKinesisWriter, Mockito.times(1)).processByteBuffer(byteBuffer, kinesisProducer, postgresConnector);
        Mockito.verify(postgresConnector, Mockito.times(1)).readPending();
        Mockito.verify(postgresConnector, Mockito.times(0)).getCurrentLSN();
        Mockito.verify(postgresConnector, Mockito.times(0)).setStreamLsn(Mockito.any(LogSequenceNumber.class));
        Mockito.verify(slotReaderKinesisWriter, Mockito.times(0)).resetIdleCounter();
    }

    @Test
    public void testReadSlotWriteToKinesisHelperNotUpdatesLsnWhenMsgNullLastFlushedLessThanIdleSlotRecreation() throws Exception {
        Mockito.doCallRealMethod().when(slotReaderKinesisWriter).readSlotWriteToKinesisHelper(kinesisProducer, postgresConnector);
        Mockito.doReturn(null).when(postgresConnector).readPending();
        Mockito.doReturn(lsn).when(postgresConnector).getCurrentLSN();
        Whitebox.setInternalState(slotReaderKinesisWriter, "lastFlushedTime", System.currentTimeMillis() - 5 * 1000);
        slotReaderKinesisWriter. readSlotWriteToKinesisHelper(kinesisProducer, postgresConnector);
        Mockito.verify(slotReaderKinesisWriter, Mockito.times(0)).processByteBuffer(byteBuffer, kinesisProducer, postgresConnector);
        Mockito.verify(postgresConnector, Mockito.times(1)).readPending();
        Mockito.verify(postgresConnector, Mockito.times(0)).getCurrentLSN();
        Mockito.verify(postgresConnector, Mockito.times(0)).setStreamLsn(Mockito.any(LogSequenceNumber.class));
        Mockito.verify(slotReaderKinesisWriter, Mockito.times(0)).resetIdleCounter();
    }

    @Test
    public void testReadSlotWriteToKinesisHelperUpdatesLsnWhenMsgNullLastFlushedGreaterThanIdleSlotRecreation() throws Exception {
        Mockito.doCallRealMethod().when(slotReaderKinesisWriter).readSlotWriteToKinesisHelper(kinesisProducer, postgresConnector);
        Mockito.doReturn(null).when(postgresConnector).readPending();
        Mockito.doReturn(lsn).when(postgresConnector).getCurrentLSN();
        Whitebox.setInternalState(slotReaderKinesisWriter, "lastFlushedTime", System.currentTimeMillis() - 11 * 1000);
        slotReaderKinesisWriter.readSlotWriteToKinesisHelper(kinesisProducer, postgresConnector);
        Mockito.verify(slotReaderKinesisWriter, Mockito.times(0)).processByteBuffer(byteBuffer, kinesisProducer, postgresConnector);
        Mockito.verify(postgresConnector, Mockito.times(2)).readPending();
        Mockito.verify(postgresConnector, Mockito.times(1)).getCurrentLSN();
        Mockito.verify(postgresConnector, Mockito.times(1)).setStreamLsn(lsn);
        Mockito.verify(slotReaderKinesisWriter, Mockito.times(1)).resetIdleCounter();
    }

    @Test
    public void testReadSlotWriteToKinesisHelperUpdatesLsnWhenMsgNullThenNotNullLastFlushedGreaterThanIdleSlotRecreation() throws Exception {
        Mockito.doCallRealMethod().when(slotReaderKinesisWriter).readSlotWriteToKinesisHelper(kinesisProducer, postgresConnector);
        Mockito.doReturn(null).doReturn(byteBuffer).when(postgresConnector).readPending();
        Mockito.doReturn(lsn).when(postgresConnector).getCurrentLSN();
        Whitebox.setInternalState(slotReaderKinesisWriter, "lastFlushedTime", System.currentTimeMillis() - 11 * 1000);
        slotReaderKinesisWriter.readSlotWriteToKinesisHelper(kinesisProducer, postgresConnector);
        Mockito.verify(slotReaderKinesisWriter, Mockito.times(1)).processByteBuffer(byteBuffer, kinesisProducer, postgresConnector);
        Mockito.verify(postgresConnector, Mockito.times(2)).readPending();
        Mockito.verify(postgresConnector, Mockito.times(1)).getCurrentLSN();
        Mockito.verify(postgresConnector, Mockito.times(1)).setStreamLsn(lsn);
        Mockito.verify(slotReaderKinesisWriter, Mockito.times(1)).resetIdleCounter();
    }

    @Test
    public void testReadSlotWriteToKinesisCatchesSqlExceptionsDestroysProducer() throws Exception {
        Mockito.doReturn("x").when(sqlException).getSQLState();
        testReadSlotWriteToKinesisException(sqlException);
    }

    @Test
    public void testReadSlotWriteToKinesisCatchesSqlExceptionsRecoveryModeSleepsDestroysProducer() throws Exception {
        Mockito.doReturn("57P03").when(sqlException).getSQLState();
        testReadSlotWriteToKinesisException(sqlException);
    }

    @Test
    public void testReadSlotWriteToKinesisCatchesIoExceptionsDestroysProducer() throws Exception {
        testReadSlotWriteToKinesisException(new IOException("io exception"));
    }

    @Test
    public void testReadSlotWriteToKinesisCatchesExceptionsDestroysProducer() throws Exception {
        testReadSlotWriteToKinesisException(new RuntimeException("exception"));
    }

    @Test
    public void testGetSlotMessageFiltersOutNonRelevantTables() throws Exception {
        Mockito.doCallRealMethod().when(slotReaderKinesisWriter).getSlotMessage(testByteArray, testByteBufferOffset);
        SlotMessage slotMessage = slotReaderKinesisWriter.getSlotMessage(testByteArray, testByteBufferOffset);
        assertEquals(slotMessage.getChange().size(), 1);
        assertEquals(slotMessage.getChange().get(0), change1);
    }

    @Test
    public void testGetUserRecordsReturnsOneUserRecordWithSlotMessageDataAndCorrectStreamName() throws Exception {
        Mockito.doCallRealMethod().when(slotReaderKinesisWriter).getUserRecords(testSlotMessage);
        ObjectMapper realObjectMapper = new ObjectMapper();
        Whitebox.setInternalState(SlotReaderKinesisWriter.class, "objectMapper", realObjectMapper);
        List<UserRecord> userRecords = slotReaderKinesisWriter.getUserRecords(testSlotMessage).collect(Collectors.toList());
        assertEquals(userRecords.size(), 1);
        SlotMessage slotMessage = realObjectMapper.readValue(userRecords.get(0).getData().array(), SlotMessage.class);
        assertEquals(slotMessage.getXid(), testSlotMessage.getXid());
    }

    @Test
    public void testConstructor() throws Exception {
        SlotReaderKinesisWriter slotReaderKinesisWriter = new SlotReaderKinesisWriter(postgresConfiguration,
                replicationConfiguration, kinesisProducerConfigurationFactory, streamName);
        assertEquals(Whitebox.getInternalState(slotReaderKinesisWriter, "postgresConfiguration"), postgresConfiguration);
        assertEquals(Whitebox.getInternalState(slotReaderKinesisWriter, "replicationConfiguration"), replicationConfiguration);
        assertEquals(Whitebox.getInternalState(slotReaderKinesisWriter, "kinesisProducerConfiguration"), kinesisProducerConfiguration);
        assertEquals(Whitebox.getInternalState(slotReaderKinesisWriter, "streamName"), streamName);
    }

    private void testReadSlotWriteToKinesisException(Exception e) throws Exception {
        Mockito.doCallRealMethod().when(slotReaderKinesisWriter).readSlotWriteToKinesis();
        Mockito.doReturn(kinesisProducer).when(slotReaderKinesisWriter).createKinesisProducer(kinesisProducerConfiguration);
        Mockito.doReturn(postgresConnector).when(slotReaderKinesisWriter).createPostgresConnector(postgresConfiguration, replicationConfiguration);
        Mockito.doThrow(e).when(slotReaderKinesisWriter).readSlotWriteToKinesisHelper(kinesisProducer, postgresConnector);
        slotReaderKinesisWriter.readSlotWriteToKinesis();
        Mockito.verify(slotReaderKinesisWriter, Mockito.times(1)).resetIdleCounter();
        Mockito.verify(slotReaderKinesisWriter, Mockito.times(1)).readSlotWriteToKinesisHelper(kinesisProducer, postgresConnector);
        Mockito.verify(slotReaderKinesisWriter, Mockito.times(1)).createKinesisProducer(kinesisProducerConfiguration);
        Mockito.verify(slotReaderKinesisWriter, Mockito.times(1)).createPostgresConnector(postgresConfiguration, replicationConfiguration);
        Mockito.verify(kinesisProducer, Mockito.times(1)).flushSync();
        Mockito.verify(kinesisProducer, Mockito.times(1)).destroy();
    }
}
