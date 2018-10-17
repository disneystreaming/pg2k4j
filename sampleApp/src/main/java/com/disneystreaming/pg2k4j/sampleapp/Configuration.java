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

package com.disney.pg2k4j.sampleapp;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.disney.pg2k4j.KinesisProducerConfigurationFactory;
import com.disney.pg2k4j.PostgresConfiguration;
import com.disney.pg2k4j.ReplicationConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Configuration implements PostgresConfiguration, ReplicationConfiguration, KinesisProducerConfigurationFactory {

    private static final Logger logger = LoggerFactory.getLogger(Configuration.class);
    private static final String awsProfileEnvVariable = "AWS_PROFILE";
    private static final String awsConfigFileEnvVariable = "AWS_CONFIG_FILE";

    private final ConfigurationSource configurationSource;

    public Configuration(ConfigurationSource configurationSource) {
        this.configurationSource = configurationSource;
    }

    private static AWSCredentialsProvider getAwsCredentialsProvider() {
        final String awsProfile = System.getenv(awsProfileEnvVariable);
        final String awsConfigLocation = System.getenv(awsConfigFileEnvVariable);

        if (awsProfile != null || awsConfigLocation != null) {
            // Connect to real AWS Kinesis Stream
            final String profile = makeProfile(awsProfile);
            return new ProfileCredentialsProvider(awsConfigLocation, profile);
        } else {
            // Connect to localstack Kinesis Stream
            return new DefaultAWSCredentialsProviderChain();
        }
    }

    private static String makeProfile(final String awsProfile) {
        final String prefix = "profile ";
        if (!awsProfile.startsWith(prefix)) {
            logger.debug("Prepending awsProfile with `{}`", prefix);
            return new StringBuilder(awsProfile).insert(0, prefix).toString();
        } else {
            return awsProfile;
        }
    }

    @Override
    public KinesisProducerConfiguration getKinesisProducerConfiguration() {
        final KinesisProducerConfiguration kinesisProducerConfig = new KinesisProducerConfiguration()
                .setCredentialsProvider(getAwsCredentialsProvider())
                .setRegion(configurationSource.region);
        if (configurationSource.KINESIS_ENDPOINT_URL != null) {
            // use localstack
            final String[] endpointPort = configurationSource.KINESIS_ENDPOINT_URL.split(":");
            kinesisProducerConfig.setKinesisEndpoint(endpointPort[endpointPort.length - 2].replace("/", ""))
                    .setKinesisPort(Long.valueOf(endpointPort[endpointPort.length - 1]))
                    .setVerifyCertificate(false);
        }
        return kinesisProducerConfig;
    }

    @Override
    public String getHost() {
        return configurationSource.pgHost;
    }

    @Override
    public String getDatabase() {
        return configurationSource.pgDatabase;
    }

    @Override
    public String getUsername() {
        return configurationSource.pgUser;
    }

    @Override
    public String getPassword() {
        return configurationSource.pgPassword;
    }

    @Override
    public String getSlotName() {
        return configurationSource.replicationSlotName;
    }

    public String getStreamName() {
        return configurationSource.kinesisStreamName;
    }
}
