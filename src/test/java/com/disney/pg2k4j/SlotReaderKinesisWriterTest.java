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
import com.disney.pg2k4j.models.Change;
import com.disney.pg2k4j.models.SlotMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Stream;

import static junit.framework.TestCase.assertEquals;


@RunWith(PowerMockRunner.class)
@PrepareForTest({SlotReaderKinesisWriter.class, Futures.class, Thread.class, SlotMessage.class})
public class SlotReaderKinesisWriterTest {

    @Mock
    private SlotReaderKinesisWriter slotReaderKinesisWriter;

    @Mock
    private PostgresConfiguration postgresConfiguration;

    @Mock
    private ReplicationConfiguration replicationConfiguration;

    @Mock
    private KinesisProducerConfiguration kinesisProducerConfiguration;

    @Mock
    private PostgresConnector postgresConnector;

    @Mock
    private KinesisProducer kinesisProducer;

    @Mock
    private ByteBuffer byteBuffer;

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

    private LogSequenceNumber lsn = LogSequenceNumber.valueOf(1234);

    private static final int testByteBufferOffset = 5;
    private static final int testIdleSlotRecreationSeconds = 10;
    private static final byte[] testByteArray = "testByteArray".getBytes();
    private static final String correctTableName = "correctTableName";
    private static final String incorrectTableName = "incorrectTableName";

    @Before
    public void setUp() throws Exception {
        PowerMockito.mockStatic(Futures.class);
        PowerMockito.mockStatic(Thread.class);
        Whitebox.setInternalState(slotReaderKinesisWriter, "replicationConfiguration", replicationConfiguration);
        Whitebox.setInternalState(slotReaderKinesisWriter, "postgresConfiguration", postgresConfiguration);
        Whitebox.setInternalState(slotReaderKinesisWriter, "kinesisProducerConfiguration", kinesisProducerConfiguration);
        Whitebox.setInternalState(SlotReaderKinesisWriter.class, "objectMapper", objectMapper);
        PowerMockito.doReturn(slotMessage).when(objectMapper).readValue(testByteArray, testByteBufferOffset, testByteArray.length, SlotMessage.class);
        PowerMockito.doReturn(testByteArray).when(byteBuffer).array();
        PowerMockito.doReturn(testByteBufferOffset).when(byteBuffer).arrayOffset();
        PowerMockito.doReturn(Stream.of(userRecord)).when(slotReaderKinesisWriter, "getUserRecords", slotMessage);
        PowerMockito.doReturn(callback).when(slotReaderKinesisWriter, "getCallback", postgresConnector, userRecord);
        PowerMockito.doReturn(slotMessage).when(slotReaderKinesisWriter, "getSlotMessage", testByteArray, testByteBufferOffset);
        PowerMockito.doReturn(future).when(kinesisProducer).addUserRecord(userRecord);
        PowerMockito.doReturn(pgReplicationStream).when(postgresConnector).getPgReplicationStream();
        PowerMockito.doReturn(testIdleSlotRecreationSeconds).when(replicationConfiguration).getUpdateIdleSlotInterval();
        PowerMockito.doReturn(correctTableName).when(change1).getTable();
        PowerMockito.doReturn(incorrectTableName).when(change2).getTable();
        PowerMockito.doCallRealMethod().when(slotMessage).getChange();
        PowerMockito.doReturn(new HashSet<>(Arrays.asList(correctTableName))).when(replicationConfiguration).getRelevantTables();
        List<Change> changes = new ArrayList<>();
        changes.add(change1);
        changes.add(change2);
        Whitebox.setInternalState(slotMessage, "change", changes);
    }

    @Test
    public void testProcessByteBufferPutsOneToKinesisAddsCallbackPerUserRecord() throws Exception {
        PowerMockito.doCallRealMethod().when(slotReaderKinesisWriter, "processByteBuffer", byteBuffer, kinesisProducer, postgresConnector);
        PowerMockito.doReturn(ByteBuffer.wrap(testByteArray)).when(userRecord).getData();
        Whitebox.invokeMethod(slotReaderKinesisWriter, "processByteBuffer", byteBuffer, kinesisProducer, postgresConnector);
        PowerMockito.verifyStatic(Futures.class,  Mockito.times(1)); // Verify that the following mock method was called exactly 1 time
        Futures.addCallback(future, callback);
        PowerMockito.verifyPrivate(slotReaderKinesisWriter, Mockito.times(1)).invoke("getCallback", postgresConnector, userRecord);
    }

    @Test
    public void testReadSlotWriteToKinesisHelperCallsProcessByteBufferWhenMsgNotNull() throws Exception {
        PowerMockito.doCallRealMethod().when(slotReaderKinesisWriter, "readSlotWriteToKinesisHelper", kinesisProducer, postgresConnector);
        PowerMockito.doReturn(byteBuffer).when(postgresConnector).readPending();
        Whitebox.invokeMethod(slotReaderKinesisWriter, "readSlotWriteToKinesisHelper", kinesisProducer, postgresConnector);
        PowerMockito.verifyPrivate(slotReaderKinesisWriter, Mockito.times(1)).invoke("processByteBuffer", byteBuffer, kinesisProducer, postgresConnector);
        Mockito.verify(postgresConnector, Mockito.times(1)).readPending();
        Mockito.verify(postgresConnector, Mockito.times(0)).getCurrentLSN();
        Mockito.verify(postgresConnector, Mockito.times(0)).setStreamLsn(Mockito.any(LogSequenceNumber.class));
        PowerMockito.verifyPrivate(slotReaderKinesisWriter, Mockito.times(0)).invoke("resetIdleCounter");
    }

    @Test
    public void testReadSlotWriteToKinesisHelperNotUpdatesLsnWhenMsgNullLastFlushedLessThanIdleSlotRecreation() throws Exception {
        PowerMockito.doCallRealMethod().when(slotReaderKinesisWriter, "readSlotWriteToKinesisHelper", kinesisProducer, postgresConnector);
        PowerMockito.doReturn(null).when(postgresConnector).readPending();
        PowerMockito.doReturn(lsn).when(postgresConnector).getCurrentLSN();
        Whitebox.setInternalState(slotReaderKinesisWriter, "lastFlushedTime", System.currentTimeMillis() - 5 * 1000);
        Whitebox.invokeMethod(slotReaderKinesisWriter, "readSlotWriteToKinesisHelper", kinesisProducer, postgresConnector);
        PowerMockito.verifyPrivate(slotReaderKinesisWriter, Mockito.times(0)).invoke("processByteBuffer", byteBuffer, kinesisProducer, postgresConnector);
        Mockito.verify(postgresConnector, Mockito.times(1)).readPending();
        Mockito.verify(postgresConnector, Mockito.times(0)).getCurrentLSN();
        Mockito.verify(postgresConnector, Mockito.times(0)).setStreamLsn(Mockito.any(LogSequenceNumber.class));
        PowerMockito.verifyPrivate(slotReaderKinesisWriter, Mockito.times(0)).invoke("resetIdleCounter");
    }

    @Test
    public void testReadSlotWriteToKinesisHelperUpdatesLsnWhenMsgNullLastFlushedGreaterThanIdleSlotRecreation() throws Exception {
        PowerMockito.doCallRealMethod().when(slotReaderKinesisWriter, "readSlotWriteToKinesisHelper", kinesisProducer, postgresConnector);
        PowerMockito.doReturn(null).when(postgresConnector).readPending();
        PowerMockito.doReturn(lsn).when(postgresConnector).getCurrentLSN();
        Whitebox.setInternalState(slotReaderKinesisWriter, "lastFlushedTime", System.currentTimeMillis() - 11 * 1000);
        Whitebox.invokeMethod(slotReaderKinesisWriter, "readSlotWriteToKinesisHelper", kinesisProducer, postgresConnector);
        PowerMockito.verifyPrivate(slotReaderKinesisWriter, Mockito.times(0)).invoke("processByteBuffer", byteBuffer, kinesisProducer, postgresConnector);
        Mockito.verify(postgresConnector, Mockito.times(2)).readPending();
        Mockito.verify(postgresConnector, Mockito.times(1)).getCurrentLSN();
        Mockito.verify(postgresConnector, Mockito.times(1)).setStreamLsn(lsn);
        PowerMockito.verifyPrivate(slotReaderKinesisWriter, Mockito.times(1)).invoke("resetIdleCounter");
    }

    @Test
    public void testReadSlotWriteToKinesisHelperUpdatesLsnWhenMsgNullThenNotNullLastFlushedGreaterThanIdleSlotRecreation() throws Exception {
        PowerMockito.doCallRealMethod().when(slotReaderKinesisWriter, "readSlotWriteToKinesisHelper", kinesisProducer, postgresConnector);
        PowerMockito.doReturn(null).doReturn(byteBuffer).when(postgresConnector).readPending();
        PowerMockito.doReturn(lsn).when(postgresConnector).getCurrentLSN();
        Whitebox.setInternalState(slotReaderKinesisWriter, "lastFlushedTime", System.currentTimeMillis() - 11 * 1000);
        Whitebox.invokeMethod(slotReaderKinesisWriter, "readSlotWriteToKinesisHelper", kinesisProducer, postgresConnector);
        PowerMockito.verifyPrivate(slotReaderKinesisWriter, Mockito.times(1)).invoke("processByteBuffer", byteBuffer, kinesisProducer, postgresConnector);
        Mockito.verify(postgresConnector, Mockito.times(2)).readPending();
        Mockito.verify(postgresConnector, Mockito.times(1)).getCurrentLSN();
        Mockito.verify(postgresConnector, Mockito.times(1)).setStreamLsn(lsn);
        PowerMockito.verifyPrivate(slotReaderKinesisWriter, Mockito.times(1)).invoke("resetIdleCounter");
    }

    @Test
    public void testReadSlotWriteToKinesisCatchesSqlExceptionsDestroysProducer() throws Exception {
        PowerMockito.doReturn("x").when(sqlException).getSQLState();
        testReadSlotWriteToKinesisException(sqlException);
    }

    @Test
    public void testReadSlotWriteToKinesisCatchesSqlExceptionsRecoveryModeSleepsDestroysProducer() throws Exception {
        PowerMockito.doReturn("57P03").when(sqlException).getSQLState();
        testReadSlotWriteToKinesisException(sqlException);
        PowerMockito.verifyStatic(Thread.class,  Mockito.times(1)); // Verify that the following mock method was called exactly 1 time
        Thread.sleep(5000);
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
        PowerMockito.doCallRealMethod().when(slotReaderKinesisWriter, "getSlotMessage", testByteArray, testByteBufferOffset);
        SlotMessage slotMessage = Whitebox.invokeMethod(slotReaderKinesisWriter, "getSlotMessage", testByteArray, testByteBufferOffset);
        assertEquals(slotMessage.getChange().size(), 1);
        assertEquals(slotMessage.getChange().get(0), change1);
    }

    private void testReadSlotWriteToKinesisException(Exception e) throws Exception {
        PowerMockito.doCallRealMethod().when(slotReaderKinesisWriter, "readSlotWriteToKinesis");

        PowerMockito.whenNew(KinesisProducer.class).withArguments(kinesisProducerConfiguration).thenReturn(kinesisProducer);
        PowerMockito.whenNew(PostgresConnector.class).withArguments(postgresConfiguration, replicationConfiguration).thenReturn(postgresConnector);
        PowerMockito.doThrow(e).when(slotReaderKinesisWriter, "readSlotWriteToKinesisHelper", kinesisProducer, postgresConnector);

        Whitebox.invokeMethod(slotReaderKinesisWriter, "readSlotWriteToKinesis");
        PowerMockito.verifyPrivate(slotReaderKinesisWriter, Mockito.times(1)).invoke("resetIdleCounter");
        PowerMockito.verifyPrivate(slotReaderKinesisWriter, Mockito.times(1)).invoke("readSlotWriteToKinesisHelper", kinesisProducer, postgresConnector);
        PowerMockito.verifyNew(KinesisProducer.class, Mockito.times(1)).withArguments(kinesisProducerConfiguration);
        PowerMockito.verifyNew(PostgresConnector.class, Mockito.times(1)).withArguments(postgresConfiguration, replicationConfiguration);
        Mockito.verify(kinesisProducer, Mockito.times(1)).flushSync();
        Mockito.verify(kinesisProducer, Mockito.times(1)).destroy();
    }
}
