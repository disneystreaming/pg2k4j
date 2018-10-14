package com.disney.pg2k4j;

import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.services.kinesis.model.StreamDescription;
import com.amazonaws.services.kinesis.model.StreamStatus;
import com.disney.pg2k4j.containers.KinesisLocalStack;
import com.disney.pg2k4j.containers.Postgres;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.Network;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;

public class KinesisReceivesPostgresChangesIT {

    private static final Network network = Network.newNetwork();

    @ClassRule
    public static Postgres postgres = new Postgres(network);

    @ClassRule
    public static KinesisLocalStack kinesisLocalStack = new KinesisLocalStack(network);

    @BeforeClass
    public static void init() throws InterruptedException, IOException, ExecutionException {
        System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true");
        System.setProperty(SDKGlobalConfiguration.AWS_CBOR_DISABLE_SYSTEM_PROPERTY, "true");
        kinesisLocalStack.createAndWait();
    }

    @Test
    public void testKinesisUp() throws SQLException {
        final StreamDescription streamDescription = kinesisLocalStack
                .getStreamDescription();

        assertEquals(KinesisLocalStack.STREAM_NAME, streamDescription.getStreamName());
        assertEquals(StreamStatus.ACTIVE.toString(), streamDescription.getStreamStatus());
        assertEquals(KinesisLocalStack.NUM_SHARDS, streamDescription.getShards().size());

        postgres.createTable();
    }
}


