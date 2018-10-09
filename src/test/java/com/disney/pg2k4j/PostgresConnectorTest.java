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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.stubbing.Answer;
import org.postgresql.PGConnection;
import org.postgresql.replication.PGReplicationConnection;
import org.postgresql.replication.PGReplicationStream;
import org.postgresql.replication.fluent.ChainedCreateReplicationSlotBuilder;
import org.postgresql.replication.fluent.logical.ChainedLogicalCreateSlotBuilder;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.Assert.*;

public class PostgresConnectorTest {

    @Mock
    ReplicationConfiguration replicationConfiguration;

    @Mock
    PGReplicationConnection pgReplicationConnection;

    @Mock
    PostgresConnector postgresConnector;

    @Mock
    PGReplicationStream pgReplicationStream;

    @Mock
    ChainedCreateReplicationSlotBuilder chainedCreateReplicationSlotBuilder;

    @Mock
    ChainedLogicalCreateSlotBuilder chainedLogicalCreateSlotBuilder;

    @Mock
    PGConnection pgConnection;

    @Mock
    private PostgresConfiguration postgresConfiguration;

    @Mock
    private Connection queryConnection;

    @Mock
    private Connection streamingConnection;

    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();

    private static final String outputPlugin = "wal2json";
    private static final String slotName = "slotName";
    private static final int tries = 2;
    private static final String postgresUrl = "postgresUrl";
    private static final Properties queryConnectionProperties = new Properties();
    private static final Properties replicationConnectionProperties = new Properties();
    private static final int sleepSeconds = 1;
    private static final PSQLException psqlException = new PSQLException("psqlException", PSQLState.OBJECT_IN_USE);
    private static final PSQLException uncaughtPsqlException = new PSQLException("psqlException", PSQLState.INVALID_CURSOR_STATE);

    @Before
    public void setUp() throws Exception {
        Mockito.doReturn(slotName).when(replicationConfiguration).getSlotName();
        Mockito.doReturn(outputPlugin).when(replicationConfiguration).getOutputPlugin();
        Mockito.doReturn(chainedCreateReplicationSlotBuilder).when(pgReplicationConnection).createReplicationSlot();
        Mockito.doReturn(chainedLogicalCreateSlotBuilder).when(chainedCreateReplicationSlotBuilder).logical();
        Mockito.doReturn(chainedLogicalCreateSlotBuilder).when(chainedLogicalCreateSlotBuilder).withOutputPlugin(outputPlugin);
        Mockito.doReturn(chainedLogicalCreateSlotBuilder).when(chainedLogicalCreateSlotBuilder).withSlotName(slotName);
        Mockito.doReturn(pgReplicationConnection).when(pgConnection).getReplicationAPI();
        Mockito.doReturn(pgConnection).when(streamingConnection).unwrap(PGConnection.class);
        Mockito.doReturn(postgresUrl).when(postgresConfiguration).getUrl();
        Mockito.doReturn(queryConnectionProperties).when(postgresConfiguration).getQueryConnectionProperties();
        Mockito.doReturn(replicationConnectionProperties).when(postgresConfiguration).getReplicationProperties();
        Mockito.doReturn(tries).when(replicationConfiguration).getExisitingProcessRetryLimit();
        Mockito.doReturn(sleepSeconds).when(replicationConfiguration).getExistingProcessRetrySleepSeconds();
        Mockito.doCallRealMethod().when(postgresConnector).getPgReplicationStream(replicationConfiguration, pgReplicationConnection);
    }

    @Test
    public void testGetPgReplicationStreamFailsAfterTwoRetries() throws Exception {
        Mockito.doThrow(psqlException).when(postgresConnector).getPgReplicationStreamHelper(replicationConfiguration, pgReplicationConnection);
        boolean thrown = false;
        PGReplicationStream localPgReplicationStream = null;
        try {
            localPgReplicationStream = postgresConnector.getPgReplicationStream(replicationConfiguration, pgReplicationConnection);
        }
        catch(PSQLException psqlException) {
            assertEquals(psqlException, PostgresConnectorTest.psqlException);
            thrown = true;
        }
        assertFalse(thrown);
        assertNull(localPgReplicationStream);
        Mockito.verify(postgresConnector, Mockito.times(2)).getPgReplicationStreamHelper(replicationConfiguration, pgReplicationConnection);
    }

    @Test
    public void testGetPgReplicationStreamReturnsAfterOneRetry() throws Exception {
        Mockito.doAnswer(new Answer<PGReplicationStream>() {
            private boolean occurred = false;
            public PGReplicationStream answer(InvocationOnMock invocationOnMock) throws Throwable {
                if (occurred) {
                    return pgReplicationStream;
                }
                else {
                    occurred = true;
                    throw psqlException;
                }
            }
        }).when(postgresConnector).getPgReplicationStreamHelper(replicationConfiguration, pgReplicationConnection);
        PGReplicationStream pgReplicationStream = postgresConnector.getPgReplicationStream(replicationConfiguration, pgReplicationConnection);
        assertEquals(pgReplicationStream, this.pgReplicationStream);
        Mockito.verify(postgresConnector, Mockito.times(2)).getPgReplicationStreamHelper(replicationConfiguration, pgReplicationConnection);
    }

    @Test
    public void testGetPgReplicationStreamFailsAfterUncatchableSqlException() throws Exception {
        Mockito.doThrow(uncaughtPsqlException).when(postgresConnector).getPgReplicationStreamHelper(replicationConfiguration, pgReplicationConnection);
        boolean thrown = false;
        PGReplicationStream localPgReplicationStream = null;
        try {
            localPgReplicationStream = postgresConnector.getPgReplicationStream(replicationConfiguration, pgReplicationConnection);
        }
        catch(PSQLException psqlException) {
            assertEquals(psqlException, this.uncaughtPsqlException);
            thrown = true;
        }
        assertTrue(thrown);
        assertNull(localPgReplicationStream);
        Mockito.verify(postgresConnector, Mockito.times(1)).getPgReplicationStreamHelper(replicationConfiguration, pgReplicationConnection);
    }

    @Test
    public void testConstructor() throws Exception {
        PostgresConnector postgresConnector = new MockPostgresConnector(postgresConfiguration, replicationConfiguration);
        assertEquals(Whitebox.getInternalState(postgresConnector, "queryConnection"), queryConnection);
        assertEquals(Whitebox.getInternalState(postgresConnector, "streamingConnection"), streamingConnection);
        assertEquals(Whitebox.getInternalState(postgresConnector, "pgReplicationStream"), pgReplicationStream);
        Mockito.verify(pgReplicationConnection, Mockito.times(1)).createReplicationSlot();
        Mockito.verify(chainedCreateReplicationSlotBuilder, Mockito.times(1)).logical();
        Mockito.verify(chainedLogicalCreateSlotBuilder, Mockito.times(1)).withOutputPlugin(outputPlugin);
        Mockito.verify(chainedLogicalCreateSlotBuilder, Mockito.times(1)).withSlotName(slotName);
        assertEquals(Whitebox.getInternalState(postgresConnector, "queryConnection"), queryConnection);
    }

    class MockPostgresConnector extends PostgresConnector {

        MockPostgresConnector(PostgresConfiguration postgresConfiguration, ReplicationConfiguration replicationConfiguration) throws SQLException {
            super(postgresConfiguration, replicationConfiguration);
        }

        @Override
        Connection createConnection(String url, Properties properties) throws SQLException {
            if (properties == queryConnectionProperties) {
                return queryConnection;
            }
            else if (properties == replicationConnectionProperties) {
                return streamingConnection;
            }
            throw new RuntimeException("Unrecognized properties");
        }

        @Override
        PGReplicationStream getPgReplicationStream(ReplicationConfiguration replicationConfiguration, PGReplicationConnection pgReplicationConnection) {
            if (replicationConfiguration == PostgresConnectorTest.this.replicationConfiguration &&
                    pgReplicationConnection == PostgresConnectorTest.this.pgReplicationConnection) {
                return pgReplicationStream;
            }
            throw new RuntimeException("Unrecognized inputs");
        }
    }

}
