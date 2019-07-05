package com.disneystreaming.pg2k4j.containers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.containers.wait.strategy.WaitStrategy;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
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
    public static final String HOST = "pghost";

    private static final Logger logger = LoggerFactory.getLogger(Postgres
            .class);

    private static final WaitStrategy postgresWaitStrategy = new
            LogMessageWaitStrategy()
            .withRegEx(".*database system is ready to accept connections.*\\s")
            .withTimes(2)
            .withStartupTimeout(Duration.of(60, SECONDS));

    public Postgres(final Network network, final String version) {
        super("debezium/postgres:" + version);

        this.withNetwork(network)
                .withExposedPorts(5432)
                .withNetworkAliases("pghost")
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
        try (final Connection con = getConnection()) {
            final Statement st = con.createStatement();
            final ResultSet rs = st.executeQuery("SELECT VERSION()");
            rs.next();
            return rs.getString(1);
        }
    }

    String getUrl() {
        return String.format("jdbc:postgresql://%s:%d/%s",
                this.getContainerIpAddress(),
                this.getFirstMappedPort(),
                DATABASE);
    }

    Connection getConnection() throws SQLException{
       return DriverManager.getConnection(getUrl(), USER, PASSWORD);
    }

    public String getHost() {
        return this.getContainerIpAddress();
    }

    public int getPort() {
        return this.getFirstMappedPort();
    }

    public void createTable() throws SQLException {
        String sql = "CREATE TABLE apples("
                + "id serial PRIMARY KEY,"
                + "name VARCHAR (50) UNIQUE NOT NULL,"
                + "quantity integer NOT NULL)";
        try (final Connection conn = getConnection()) {
            Statement st = conn.createStatement();
            st.execute(sql);
        }
    }

    public void insertApple(String name, int quantity) throws SQLException {
        String sql = "INSERT INTO apples(name, quantity) VALUES(? , ?)";
        try (final Connection conn = getConnection()) {
            try (final PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, name);
                ps.setInt(2, quantity);
                ps.execute();
            }
        }
    }

    public void deleteApple(String name) throws SQLException {
        String sql = "DELETE FROM apples WHERE name = ?";
        try (final Connection conn = getConnection()) {
            try (final PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, name);
                ps.execute();
            }
        }
    }

    public void updateApple(String name, int quantity) throws SQLException {
        String sql = "UPDATE apples SET quantity = ? WHERE name = ?";
        try (final Connection conn = getConnection()) {
            try (final PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(2, name);
                ps.setInt(1, quantity);
                ps.execute();
            }
        }
    }
}
