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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.postgresql.replication.PGReplicationConnection;
import org.postgresql.replication.PGReplicationStream;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import static org.junit.Assert.*;


@RunWith(PowerMockRunner.class)
@PrepareForTest(PostgresConnector.class)
public class PostgresConnectorTest {

    @Mock
    ReplicationConfiguration replicationConfiguration;

    @Mock
    PGReplicationConnection pgReplicationConnection;

    @Mock
    PostgresConnector postgresConnector;

    @Mock
    PGReplicationStream pgReplicationStream;

    private static final int tries = 2;
    private static final int sleepSeconds = 1;
    private static final PSQLException psqlException = new PSQLException("psqlException", PSQLState.OBJECT_IN_USE);
    private static final PSQLException uncaughtPsqlException = new PSQLException("psqlException", PSQLState.INVALID_CURSOR_STATE);


    @Before
    public void setUp() throws Exception {
        PowerMockito.doReturn(tries).when(replicationConfiguration).getExisitingProcessRetryLimit();
        PowerMockito.doReturn(sleepSeconds).when(replicationConfiguration).getExistingProcessRetrySleepSeconds();
        PowerMockito.doCallRealMethod().when(postgresConnector, "getPgReplicationStream", replicationConfiguration, pgReplicationConnection);
    }

    @Test
    public void testGetPgReplicationStreamReturnedAfterOneRetry() throws Exception {
        PowerMockito.doAnswer(new Answer<PGReplicationStream>() {
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
        }).when(postgresConnector, "getPgReplicationStreamHelper", replicationConfiguration, pgReplicationConnection);
        PGReplicationStream pgReplicationStream = Whitebox.invokeMethod(postgresConnector, "getPgReplicationStream", replicationConfiguration, pgReplicationConnection);
        assertEquals(pgReplicationStream, this.pgReplicationStream);
        PowerMockito.verifyPrivate(postgresConnector, Mockito.times(2)).invoke( "getPgReplicationStreamHelper", replicationConfiguration, pgReplicationConnection);
    }

    @Test
    public void testGetPgReplicationStreamFailsAfterTwoRetries() throws Exception {
        PowerMockito.doThrow(psqlException).when(postgresConnector, "getPgReplicationStreamHelper", replicationConfiguration, pgReplicationConnection);
        boolean thrown = false;
        PGReplicationStream localPgReplicationStream = null;
        try {
            localPgReplicationStream = Whitebox.invokeMethod(postgresConnector, "getPgReplicationStream", replicationConfiguration, pgReplicationConnection);
        }
        catch(PSQLException psqlException) {
            assertEquals(psqlException, this.psqlException);
            thrown = true;
        }
        assertFalse(thrown);
        assertNull(localPgReplicationStream);
        PowerMockito.verifyPrivate(postgresConnector, Mockito.times(2)).invoke( "getPgReplicationStreamHelper", replicationConfiguration, pgReplicationConnection);
    }

    @Test
    public void testGetPgReplicationStreamFailsAfterUncatchableSqlException() throws Exception {
        PowerMockito.doThrow(uncaughtPsqlException).when(postgresConnector, "getPgReplicationStreamHelper", replicationConfiguration, pgReplicationConnection);
        boolean thrown = false;
        PGReplicationStream localPgReplicationStream = null;
        try {
            localPgReplicationStream = Whitebox.invokeMethod(postgresConnector, "getPgReplicationStream", replicationConfiguration, pgReplicationConnection);
        }
        catch(PSQLException psqlException) {
            assertEquals(psqlException, this.uncaughtPsqlException);
            thrown = true;
        }
        assertTrue(thrown);
        assertNull(localPgReplicationStream);
        PowerMockito.verifyPrivate(postgresConnector, Mockito.times(1)).invoke( "getPgReplicationStreamHelper", replicationConfiguration, pgReplicationConnection);
    }

}
