package com.disney.pg2k4j.containers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.containers.wait.strategy.WaitStrategy;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;

import static java.time.temporal.ChronoUnit.SECONDS;

public class Postgres<SELF extends GenericContainer<SELF>> extends
        GenericContainer<SELF> {

    public static final String USER = "postgres";
    public static final String PASSWORD = "postgres";
    public static final String DATABASE = "test";

    private static final Logger logger = LoggerFactory.getLogger(Postgres
            .class);

    private static final WaitStrategy postgresWaitStrategy = new
            LogMessageWaitStrategy()
            .withRegEx(".*database system is ready to accept connections.*\\s")
            .withTimes(2)
            .withStartupTimeout(Duration.of(60, SECONDS));

    public Postgres(final Network network) {
        super("debezium/postgres:10.0");

        this.withNetwork(network)
                .withExposedPorts(5432)
                .withNetworkAliases("post.gres")
                .withClasspathResourceMapping("pg_hba.conf", "/etc/pg_hba"
                        + ".conf", BindMode.READ_ONLY)
                .withEnv("POSTGRES_USER", USER)
                .withEnv("POSTGRES_PASSWORD", PASSWORD)
                .withEnv("POSTGRES_DB", DATABASE)
                .withEnv("EXTRA_OPTS", "-c hba_file=/etc/pg_hba.conf -c "
                        + "max_wal_senders=3 -c wal_level=logical -c "
                        + "max_replication_slots=1")
                .waitingFor(postgresWaitStrategy);
    }

    public final String getVersion() throws SQLException {
        final String url = String.format("jdbc:postgresql://%s:%d/%s",
                this.getContainerIpAddress(),
                this.getFirstMappedPort(),
                DATABASE);

        logger.info("Getting version for DB : {}", url);

        try (final Connection con = DriverManager.getConnection(url, USER,
                PASSWORD);
             final Statement st = con.createStatement();
             final ResultSet rs = st.executeQuery("SELECT VERSION()")) {

            rs.next();
            return rs.getString(1);
        }
    }

    public String getHost() {
        return this.getContainerIpAddress();
    }

    public int getPort() {
        return this.getFirstMappedPort();
    }

}
