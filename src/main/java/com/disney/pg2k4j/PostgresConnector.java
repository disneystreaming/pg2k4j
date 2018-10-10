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

import org.postgresql.PGConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationConnection;
import org.postgresql.replication.PGReplicationStream;
import org.postgresql.util.PSQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class PostgresConnector implements AutoCloseable {

    private static final String alreadyExistsSqlState = "42710";
    private static final String currentlyRunningProcessOnSlotSqlState = "55006";

    private static final Logger logger =
            LoggerFactory.getLogger(PostgresConnector.class);

    private final Connection queryConnection;
    private final Connection streamingConnection;
    private final PGReplicationStream pgReplicationStream;

    /**
     * Initializes {@link #queryConnection} which {@link #getCurrentLSN()} uses
     * Initializes {@link #streamingConnection} for opening up
     * {@link #pgReplicationStream}
     * Initializes {@link #pgReplicationStream} for reading the replication slot
     *
     * @param postgresConfiguration
     * @param replicationConfiguration
     * @throws SQLException
     */
    PostgresConnector(final PostgresConfiguration postgresConfiguration,
                      final ReplicationConfiguration replicationConfiguration)
            throws SQLException {
        logger.debug("Connecting to {}", postgresConfiguration.getUrl());
        queryConnection = createConnection(postgresConfiguration.getUrl(),
                postgresConfiguration.getQueryConnectionProperties());
        streamingConnection = createConnection(postgresConfiguration.getUrl(),
                postgresConfiguration.getReplicationProperties());
        logger.debug("Connected to postgres");
        PGConnection pgConnection =
                streamingConnection.unwrap(PGConnection.class);
        PGReplicationConnection pgReplicationConnection =
                pgConnection.getReplicationAPI();
        try {
            logger.info("Attempting to create replication slot {}",
                    replicationConfiguration.getSlotName());
            pgReplicationConnection.createReplicationSlot()
                    .logical()
                    .withOutputPlugin(
                            replicationConfiguration.getOutputPlugin())
                    .withSlotName(replicationConfiguration.getSlotName())
                    .make();
            logger.info("Created replication slot");
        } catch (SQLException e) {
            if (e.getSQLState().equals(alreadyExistsSqlState)) {
                logger.info("Slot {} already exists",
                        replicationConfiguration.getSlotName());
            } else {
                throw(e);
            }
        }
        pgReplicationStream = getPgReplicationStream(replicationConfiguration,
                pgReplicationConnection);
    }

    public PGReplicationStream getPgReplicationStream() {
        return pgReplicationStream;
    }

    public ByteBuffer readPending() throws SQLException {
        return pgReplicationStream.readPending();
    }

    public LogSequenceNumber getCurrentLSN() throws SQLException {
        try (Statement st = queryConnection.createStatement()) {
            try (ResultSet rs =
                      st.executeQuery("select pg_current_wal_lsn()")) {
                if (rs.next()) {
                    String lsn = rs.getString(1);
                    return LogSequenceNumber.valueOf(lsn);
                } else {
                    return LogSequenceNumber.INVALID_LSN;
                }
            }
        }
    }

    public void setStreamLsn(final LogSequenceNumber lsn) {
        pgReplicationStream.setAppliedLSN(lsn);
        pgReplicationStream.setFlushedLSN(lsn);
    }

    public LogSequenceNumber getLastReceivedLsn() {
        return pgReplicationStream.getLastReceiveLSN();
    }

    @Override
    public void close() {
        if (pgReplicationStream != null) {
            try {
                if (!pgReplicationStream.isClosed()) {
                    pgReplicationStream.forceUpdateStatus();
                    pgReplicationStream.close();
                }
            } catch (SQLException sqlException) {
                logger.error("Unable to close replication stream",
                        sqlException);
            }
        }
        if (streamingConnection != null) {
            try {
                streamingConnection.close();
            } catch (SQLException sqlException) {
                logger.error("Unable to close postgres streaming connection",
                        sqlException);
            }
        }
        if (queryConnection != null) {
            try {
                queryConnection.close();
            } catch (SQLException sqlException) {
                logger.error("Unable to close postgres query connection",
                        sqlException);
            }
        }
    }

    /**
     * Retry initializing the stream according to settings in
     * ReplicationConfiguration. Do this because only one PID
     * may consume from a slot at once. In the case of blue/green deploys,
     * there may be some overlap between the old deployment spinning down
     * and the new one spinning up. The retries allow the newly
     * spun up deployment to wait for the previous deployment to spin down.
     * @param replicationConfiguration
     * @param pgReplicationConnection
     * @throws SQLException
     */
    PGReplicationStream getPgReplicationStream(
            final ReplicationConfiguration replicationConfiguration,
            final PGReplicationConnection pgReplicationConnection)
            throws SQLException {
        boolean listening = false;
        int tries = replicationConfiguration.getExisitingProcessRetryLimit();
        PGReplicationStream pgRepStream = null;
        while (!listening && tries > 0) {
            try {
                pgRepStream = getPgReplicationStreamHelper(
                        replicationConfiguration, pgReplicationConnection);
                listening = true;
            } catch (PSQLException psqlException) {
                if (psqlException.getSQLState()
                        .equals(currentlyRunningProcessOnSlotSqlState)) {
                    logger.info("Replication slot currently "
                            + "has another process consuming from it");
                    tries -= 1;
                    if (tries > 0) {
                        logger.info("Sleeping for {} seconds"
                                       + " before retrying {} more times",
                                replicationConfiguration.
                                        getExistingProcessRetrySleepSeconds(),
                                tries, psqlException);
                        try {
                            Thread.sleep(TimeUnit.MILLISECONDS
                                    .toSeconds(replicationConfiguration
                                       .getExistingProcessRetrySleepSeconds()));
                        } catch (InterruptedException ie) {
                            logger.info("Received interruption while "
                                    + "attempting to setup replciation stream");
                            tries = 0;
                        }
                    }
                } else {
                    throw psqlException;
                }
            }
        }
        return pgRepStream;
    }

    PGReplicationStream getPgReplicationStreamHelper(
            final ReplicationConfiguration replicationConfiguration,
            final PGReplicationConnection pgReplicationConnection)
            throws SQLException {
        return pgReplicationConnection
                .replicationStream()
                .logical()
                .withStatusInterval(replicationConfiguration
                        .getStatusIntervalValue(),
                        replicationConfiguration.getStatusIntervalTimeUnit())
                .withSlotOptions(replicationConfiguration.getSlotOptions())
                .withSlotName(replicationConfiguration.getSlotName()).start();
    }

    Connection createConnection(final String url, final Properties properties)
            throws SQLException {
        return DriverManager.getConnection(url, properties);
    }
}
