package com.disney.pg2k4j;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

public class CommandLineRunner implements
        PostgresConfiguration,
        ReplicationConfiguration,
        KinesisProducerConfigurationFactory {

    private static final Logger logger = LoggerFactory.getLogger(
            CommandLineRunner.class);

    public static void main(final String[] args) {
        final CommandLineRunner commandLineRunner = new CommandLineRunner();
        new CommandLine(commandLineRunner).parseArgs(args);
        new SlotReaderKinesisWriter(
                commandLineRunner,
                commandLineRunner,
                commandLineRunner,
                commandLineRunner.streamName
        ).runLoop();
    }

    @CommandLine.Option(
            names = {"-p", "--pgport"},
            description = "Port that the postgres server is running on",
            defaultValue = "5432"
    )
    private String pgPort;

    @CommandLine.Option(
            names = {"-h", "--pghost"},
            description = "Host that the postgres server is running on",
            required = true
    )
    private String pgHost;

    @CommandLine.Option(
            names = {"-u", "--pguser"},
            description = "Username for the postgres server",
            required = true
    )
    private String pgUser;

    @CommandLine.Option(
            names = {"-x", "--pgpassword"},
            description = "Password for the postgres server",
            required = true
    )
    private String pgPassword;

    @CommandLine.Option(
            names = {"-d", "--pgdatabase"},
            description = "Database of the postgres server",
            required = true
    )
    private String pgDatabase;

    @CommandLine.Option(
            names = {"-s", "--streamName"},
            description = "The name of the kinesis stream",
            required = true
    )
    private String streamName;

    @CommandLine.Option(
            names = {"-a", "--awsProfile"},
            description = "AWS Profile to use for accessing the Kinesis Stream."
                    + " If one is provided a ProfileCredentialProvider will"
                    + " be used for interacting with AWS. Otherwise the"
                    + " DefaultAWSCredentialsProviderChain will be used."
    )
    private String awsProfile;

    @CommandLine.Option(
            names = {"-c", "--awsConfigLocation"},
            description = "File path to use for sourcing AWS config."
                    + " If one is provided a ProfileCredentialProvider will"
                    + " be used for interacting with AWS. Otherwise the"
                    + " DefaultAWSCredentialsProviderChain will be used."
    )
    private String awsConfigLocation;

    @CommandLine.Option(
            names = {"-r", "--region"},
            description = "AWS region in which the Kinesis Stream is located."
    )
    private String region;

    @CommandLine.Option(
            names = {"-k", "--kinesisEndpoint"},
            description = "Endpoint to use for interacting with Kinesis. Set"
                    + " this if configuring pg2k4j against localstack kinesis."
    )
    private String kinesisEndpoint;

    @CommandLine.Option(
            names = {"-n", "--slotName"},
            description = "Slot name to use when reading Postgres changes.",
            required = false,
            defaultValue = "pg2k4j"
    )
    private String slotName;

    private AWSCredentialsProvider getAwsCredentialsProvider() {
        if (awsProfile != null || awsConfigLocation != null) {
            final String profile = makeProfile(awsProfile);
            return new ProfileCredentialsProvider(awsConfigLocation, profile);
        } else {
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
        final KinesisProducerConfiguration kinesisProducerConfig =
                new KinesisProducerConfiguration()
                .setCredentialsProvider(getAwsCredentialsProvider())
                .setRegion(region);
        if (kinesisEndpoint != null) {
            final String[] endpointPort = kinesisEndpoint.split(":");
            kinesisProducerConfig.setKinesisEndpoint(
                    endpointPort[endpointPort.length - 2].replace("/", ""))
                    .setKinesisPort(
                            Long.valueOf(endpointPort[endpointPort.length - 1]))
                    .setVerifyCertificate(false);
        }
        return kinesisProducerConfig;
    }

    @Override
    public String getSlotName() {
        return slotName;
    }

    @Override
    public String getHost() {
        return pgHost;
    }

    @Override
    public String getDatabase() {
        return pgDatabase;
    }

    @Override
    public String getUsername() {
        return pgUser;
    }

    @Override
    public String getPassword() {
        return pgPassword;
    }
}
