package com.disney.pg2k4j;

import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.Record;
import com.disney.pg2k4j.containers.KinesisLocalStack;
import com.disney.pg2k4j.containers.Postgres;
import com.disney.pg2k4j.models.Change;
import com.disney.pg2k4j.models.InsertChange;
import com.disney.pg2k4j.models.SlotMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class KinesisReceivesPostgresChangesIT {

    private static final Logger logger =
            LoggerFactory.getLogger(KinesisReceivesPostgresChangesIT.class);

    private static final Network network = Network.newNetwork();
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static Thread t;

    @ClassRule
    public static Postgres postgres = new Postgres(network);

    @ClassRule
    public static KinesisLocalStack kinesisLocalStack = new KinesisLocalStack(network);

    @BeforeClass
    public static void init() throws InterruptedException, IOException, ExecutionException, SQLException {
        System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true");
        System.setProperty(SDKGlobalConfiguration.AWS_CBOR_DISABLE_SYSTEM_PROPERTY, "true");
        kinesisLocalStack.createAndWait();
        postgres.createTable();
        CommandLineRunner commandLineRunner = CommandLineRunner.initialize(
                new String[]{"-p", String.valueOf(postgres.getPort()), "-h",
                        postgres.getHost(), "-u", Postgres.USER, "-x",
                        Postgres.PASSWORD, "-d", "test", "-s",
                        KinesisLocalStack.STREAM_NAME, "-k",
                        kinesisLocalStack.getEndpoint(), "-e", "test",
                        "-f", "test"}
        );
        t = new Thread(commandLineRunner);
        t.start();
    }

    @Test
    public void testCDC() throws Exception {
        kinesisLocalStack.getAllRecords();
        postgres.insertRecords();
        int attempts = 0;
        boolean seenFuji = false;
        boolean seenGala = false;
        while (attempts < 10) {
            GetRecordsResult getRecordsResult = kinesisLocalStack
                    .getAllRecords();
            List<Record> records = getRecordsResult.getRecords();
            for(Record record: records) {
                SlotMessage s = objectMapper.readValue(record.getData().array(), SlotMessage.class);
                for (Change c: s.getChange()) {
                    if (c instanceof InsertChange) {
                        InsertChange ic = (InsertChange) c;
                        if (ic.getColumnvalues().equals(Arrays.asList(1, "Fuji", 3))) {
                            seenFuji = true;
                        }
                        else if (ic.getColumnvalues().equals(Arrays.asList(2, "Gala", 10))) {
                            seenGala = true;
                        }
                        else {
                            throw new Exception(String.format("Unrecognized columvalues on stream %s", ic.getColumnvalues()));
                        }
                        assertEquals(c.getKind(), "insert");
                        assertEquals(c.getTable(), "apples");
                        assertEquals(c.getSchema(), "public");
                        assertEquals(c.getColumnnames(), Arrays.asList("id", "name", "quantity"));
                    }
                    else {
                        throw new Exception(String.format("Unrecognized change type on stream %s", c.getKind()));
                    }
                }
            }
            if (seenFuji && seenGala) {
                break;
            }
            logger.info("Yet to find CDC Records on Stream."
                    + " Sleeping for a second then retrying.");
            Thread.sleep(1000);
            attempts += 1;
        }
        assertTrue(seenFuji);
        assertTrue(seenGala);
    }


}


