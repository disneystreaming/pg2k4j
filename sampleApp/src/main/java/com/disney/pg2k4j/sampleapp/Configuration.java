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

    @Override
    public KinesisProducerConfiguration getKinesisProducerConfiguration() {
        final KinesisProducerConfiguration kinesisProducerConfig = new KinesisProducerConfiguration()
                    .setCredentialsProvider(getAwsCredentialsProvider())
                    .setRegion(configurationSource.region);
            if (configurationSource.KINESIS_ENDPOINT_URL != null) {
                // use localstack
                final String[] endpointPort = KINESIS_ENDPOINT_URL.split(":");
                kinesisProducerConfig.setKinesisEndpoint(endpointPort[endpointPort.length - 2].replace("/", ""))
                        .setKinesisPort(Long.valueOf(endpointPort[endpointPort.length - 1]))
                        .setVerifyCertificate(false);
            }
            return kinesisProducerConfig;
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
