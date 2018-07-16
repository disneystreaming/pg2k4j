package com.disney.pg2k4j;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public interface ReplicationConfiguration {

    static final int DEFAULT_STATUS_INTERVAL_VALUE = 20;
    static final TimeUnit DEFAULT_STATUS_INTERVAL_TIME_UNIT = TimeUnit.SECONDS;
    static final boolean DEFAULT_INCLUDE_XIDS = true;
    static final String DEFAULT_OUTPUT_PLUGIN = "wal2json";
    static final int DEFAULT_UPDATE_IDLE_SLOT_INTERVAL = 300;
    static final int DEFAULT_EXISTING_PROCESS_RETRY_LIMIT = 30;
    static final int DEFAULT_EXISTING_PROCESS_RETRY_SLEEP_SECONDS = 30;

    String getSlotName();

    default int getStatusIntervalValue() {
        return DEFAULT_STATUS_INTERVAL_VALUE;
    }

    default TimeUnit getStatusIntervalTimeUnit() {
        return DEFAULT_STATUS_INTERVAL_TIME_UNIT;
    }

    default boolean getIncludeXids() {
        return DEFAULT_INCLUDE_XIDS;
    }

    default String getOutputPlugin() {
        return DEFAULT_OUTPUT_PLUGIN;
    }

    default Properties getSlotOptions() {
        Properties properties = new Properties();
        properties.setProperty("include-xids", String.valueOf(getIncludeXids()));
        return properties;
    }

    default int getUpdateIdleSlotInterval() {
        return DEFAULT_UPDATE_IDLE_SLOT_INTERVAL;
    }

    default int getExisitingProcessRetryLimit() {
        return DEFAULT_EXISTING_PROCESS_RETRY_LIMIT;
    }

    default int getExistingProcessRetrySleepSeconds() {
        return DEFAULT_EXISTING_PROCESS_RETRY_SLEEP_SECONDS;
    }

    default Set<String> getRelevantTables() {
        return null;
    }
}
