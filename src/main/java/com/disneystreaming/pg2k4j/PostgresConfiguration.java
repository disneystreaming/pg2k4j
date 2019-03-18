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

package com.disneystreaming.pg2k4j;

import org.postgresql.PGProperty;
import java.util.Properties;

public interface PostgresConfiguration {

    String DEFAULT_PORT = "5432";
    String MIN_SERVER_VERSION = "10.3";
    String DEFAULT_SSL_MODE = "disable";

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
        return String.format("jdbc:postgresql://%s:%s/%s", getHost(),
                getPort(), getDatabase());
    }

    default String getPathToRootCert() {
        return null;
    }

    default String getSslPassword() {
        return null;
    }

    default String getPathToSslKey() {
        return null;
    }

    default String getPathToSslCert() {
        return null;
    }

    default String getSslMode() {
        return DEFAULT_SSL_MODE;
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
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(properties,
                getMinServerVersion());
        PGProperty.SSL_MODE.set(properties, getSslMode());
        PGProperty.SSL_ROOT_CERT.set(properties, getPathToRootCert());
        PGProperty.SSL_CERT.set(properties, getPathToSslCert());
        PGProperty.SSL_PASSWORD.set(properties, getSslPassword());
        PGProperty.SSL_KEY.set(properties, getPathToSslKey());
        return properties;
    }

    String getHost();

    String getDatabase();

    String getUsername();

    String getPassword();

}
