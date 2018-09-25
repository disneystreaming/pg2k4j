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
