package com.disneystreaming.pg2k4j.containers;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.waiters.FixedDelayStrategy;
import com.amazonaws.waiters.MaxAttemptsRetryStrategy;
import com.amazonaws.waiters.PollingStrategy;
import com.amazonaws.waiters.WaiterParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.testcontainers.containers.BindMode.READ_WRITE;

public class KinesisDynamoLocalStack<SELF extends GenericContainer<SELF>> extends
        GenericContainer<SELF> {

    private static final int KINESIS_PORT = 4568;
    private static final int DYNAMO_PORT = 4569;
    private static final String KINESIS_SERVICE_NAME = "kinesis";
    private static final String DYNAMO_SERVICE_NAME = "dynamodb";
    public static final String STREAM_NAME = "postgres_cdc";
    public static final int NUM_SHARDS = 1;
    public AmazonKinesis client;

    private static final Logger logger =
            LoggerFactory.getLogger(KinesisDynamoLocalStack.class);

    public KinesisDynamoLocalStack(final Network network) {
        super("localstack/localstack:latest");

        this.withNetwork(network)
                .withNetworkAliases("localstack")
                .withExposedPorts(KINESIS_PORT, DYNAMO_PORT)
                .withFileSystemBind("/var/run/docker.sock", "/var/run/docker"
                        + ".sock", READ_WRITE)
                .withEnv("USE_SSL", "true")
                .withEnv("SERVICES",
                        KINESIS_SERVICE_NAME + "," + DYNAMO_SERVICE_NAME)
                .waitingFor(new LogMessageWaitStrategy().withRegEx(".*Ready\\"
                        + ".\n"));
    }

    public AwsClientBuilder.EndpointConfiguration getEndpointConfiguration
            (boolean kinesis) {
        final StringBuilder host = new StringBuilder();
        final String containerIpAddress = this.getContainerIpAddress();

        try {
            host.append(InetAddress.getByName(containerIpAddress)
                    .getHostAddress());
        } catch (UnknownHostException ignored) {
            logger.info("Cannot resolve container host address - reverting to"
                    + " container IP");
            host.append(containerIpAddress);
        }
        final int mappedPort;
        if (kinesis) {
            mappedPort = this.getMappedPort(KINESIS_PORT);
        } else {
            mappedPort = this.getMappedPort(DYNAMO_PORT);
        }

        final String endpoint = String.format("https://%s:%d", host.toString
                (), mappedPort);

        logger.info("Building {} with endpoint {}",
                kinesis ? KINESIS_SERVICE_NAME : DYNAMO_SERVICE_NAME,
                endpoint);
        return new AwsClientBuilder.EndpointConfiguration(endpoint,
                "us-east-1");
    }

    public AWSCredentialsProvider getDefaultCredentialsProvider() {
        return new AWSStaticCredentialsProvider(new BasicAWSCredentials
                ("test", "test"));
    }

    public void createAndWait() {
        client = AmazonKinesisClientBuilder
            .standard()
            .withEndpointConfiguration(getEndpointConfiguration(true))
            .withCredentials(getDefaultCredentialsProvider())
            .build();


        client.createStream(STREAM_NAME, NUM_SHARDS);

        final DescribeStreamRequest describeStreamRequest =
                new DescribeStreamRequest().withStreamName(STREAM_NAME);
        final PollingStrategy pollingStrategy = new
                PollingStrategy(new MaxAttemptsRetryStrategy(
                        25),
                new FixedDelayStrategy(5));

        final WaiterParameters<DescribeStreamRequest> waiterParameters =
                new WaiterParameters<>(describeStreamRequest)
                        .withPollingStrategy(pollingStrategy);

        client.waiters().streamExists().run(waiterParameters);
    }

    public String getKinesisEndpoint() {
        return String.format("http://%s:%d",
                this.getContainerIpAddress(),
                this.getFirstMappedPort());
    }

    public String getDynamoEndpoint() {
        return String.format("http://%s:%d",
                this.getContainerIpAddress(),
                this.getMappedPort(DYNAMO_PORT));
    }

}

