package com.disney.pg2k4j;

import org.postgresql.PGProperty;

import java.util.Properties;

public interface PostgresConfiguration {

    static final String DEFAULT_PORT = "5432";
    static final String MIN_SERVER_VERSION = "10.3";

    default String getPort() {
        return DEFAULT_PORT;
    }

    default String getMinServerVersion() {
        return MIN_SERVER_VERSION;
    }

    default String getReplication() {
        return "database";
    }

    default String getQueryMode() {
        return "simple";
    }

    default String getUrl() {
        return String.format("jdbc:postgresql://%s:%s/%s", getHost(), getPort(), getDatabase());
    }

    default Properties getReplicationProperties() {
        Properties properties = getQueryConnectionProperties();
        PGProperty.PREFER_QUERY_MODE.set(properties, getQueryMode());
        PGProperty.REPLICATION.set(properties, getReplication());
        return properties;
    }

    default Properties getQueryConnectionProperties() {
        Properties properties = new Properties();
        PGProperty.USER.set(properties, getUsername());
        PGProperty.PASSWORD.set(properties, getPassword());
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(properties, getMinServerVersion());
        return properties;
    }

    public String getHost();

    public String getDatabase();

    public String getUsername();

    public String getPassword();

}
