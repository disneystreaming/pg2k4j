package com.disneystreaming.pg2k4j;

import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.disneystreaming.pg2k4j.containers.KinesisDynamoLocalStack;
import com.disneystreaming.pg2k4j.containers.Postgres;
import com.disneystreaming.pg2k4j.models.Change;
import com.disneystreaming.pg2k4j.models.DeleteChange;
import com.disneystreaming.pg2k4j.models.InsertChange;
import com.disneystreaming.pg2k4j.models.SlotMessage;
import com.disneystreaming.pg2k4j.models.UpdateChange;
import com.disneystreaming.pg2k4j.testresources
        .SlotMessageRecordProcessorFactory;
import com.google.common.collect.ImmutableMap;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class KinesisReceivesPostgresChangesIT {

    private static final Logger logger =
            LoggerFactory.getLogger(KinesisReceivesPostgresChangesIT.class);

    private static final Network network = Network.newNetwork();
    private static Thread slotReaderKinesisWriterThread;
    private static Thread kclThread;
    private static final int GET_RECORDS_ATTEMPTS = 15;
    private static SlotMessageRecordProcessorFactory
            slotMessageRecordProcessorFactory;

    @ClassRule
    public static Postgres postgres = new Postgres(network);

    @ClassRule
    public static KinesisDynamoLocalStack kinesisDynamoLocalStack =
            new KinesisDynamoLocalStack(network);

    @BeforeClass
    public static void init() throws Exception {
        System.setProperty(
                SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY,
                "true");
        System.setProperty(
                SDKGlobalConfiguration.AWS_CBOR_DISABLE_SYSTEM_PROPERTY,
                "true");
        kinesisDynamoLocalStack.createAndWait();
        postgres.createTable();
        slotMessageRecordProcessorFactory = new
                SlotMessageRecordProcessorFactory(
                        kinesisDynamoLocalStack.getEndpointConfiguration(false).getServiceEndpoint(),
                kinesisDynamoLocalStack.getEndpointConfiguration(true).getServiceEndpoint(),
                kinesisDynamoLocalStack.STREAM_NAME);
        CommandLineRunner commandLineRunner = CommandLineRunner.initialize(
                new String[]{
                        "--pgport", String.valueOf(postgres.getPort()),
                        "--pghost", postgres.getHost(),
                        "--pguser", Postgres.USER,
                        "--pgpassword", Postgres.PASSWORD,
                        "--pgdatabase", "test",
                        "--streamname", KinesisDynamoLocalStack.STREAM_NAME,
                        "--kinesisendpoint",
                        kinesisDynamoLocalStack.getKinesisEndpoint(),
                        "--awsaccesskey", "test",
                        "--awssecret", "test"
                }
        ).get();
        kclThread = new Thread(slotMessageRecordProcessorFactory);
        kclThread.start();
        slotMessageRecordProcessorFactory.waitForConsumer();
        slotReaderKinesisWriterThread = new Thread(commandLineRunner);
        slotReaderKinesisWriterThread.start();
        logger.info("Infrastructure initialized, starting tests...");
        Thread.sleep(3000);
    }

    @Test
    public void testInsertAndDeleteForwardedToStream() throws Exception {
        Map<String, Integer> appleNamesToQuantities = ImmutableMap.of(
                "Fuji", 2,
                "Gala", 3
        );
        insertRecords(appleNamesToQuantities);
        deleteRecords(appleNamesToQuantities);
        verifyPostgresInsertRecordsAppearOnKinesisStream(
                appleNamesToQuantities);
        verifyPostgresDeleteRecordsAppearOnKinesisStream(
                appleNamesToQuantities);
    }

    @Test
    public void testInsertAndUpdateForwardedToStream() throws Exception {
        Map<String, Integer> appleNamesToQuantitiesInsert = ImmutableMap.of(
                "Macintosh", 5,
                "Granny Smith", 7
        );
        Map<String, Integer> appleNamesToQuantitiesUpdate = ImmutableMap.of(
                "Macintosh", 1
        );
        insertRecords(appleNamesToQuantitiesInsert);
        updateRecords(appleNamesToQuantitiesUpdate);
        verifyPostgresInsertRecordsAppearOnKinesisStream(
                appleNamesToQuantitiesInsert);
        verifyPostgresUpdateRecordsAppearOnKinesisStream(
                appleNamesToQuantitiesUpdate);
    }


    void insertRecords(Map<String, Integer> appleNamesToQuantities) {
        appleNamesToQuantities.forEach((k, v) -> {
            try {
                postgres.insertApple(k, v);
            } catch(Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    void deleteRecords(Map<String, Integer> appleNamesToQuantities) {
        appleNamesToQuantities.keySet().forEach(k -> {
            try {
                postgres.deleteApple(k);
            } catch(Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    void updateRecords(Map<String, Integer> appleNamesToQuantities) {
        appleNamesToQuantities.forEach((k, v) -> {
            try {
                postgres.updateApple(k, v);
            } catch(Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    void verifyPostgresRecordsAppearOnKinesisStream(
            Map<String, Integer> appleNamesToQuantities,
            String kind) throws Exception {
        int attempts = 0;
        Map<String, Boolean> seenMap;
        int deleteCounts = 0;
        seenMap  = new HashMap<>();
        appleNamesToQuantities.keySet().forEach(k -> seenMap.put(k, false));
        boolean seenAll = false;
        while (!seenAll && attempts < GET_RECORDS_ATTEMPTS) {
            logger.info("Yet to find CDC Records on Stream."
                    + " Sleeping for a second then retrying.");
            Thread.sleep(1000);
            attempts += 1;
            for(SlotMessage s: slotMessageRecordProcessorFactory
                    .slotMessageRecordProcessor.slotMessages) {
                    for (Change c : s.getChange()) {
                        if (c.getKind().equals("insert")) {
                            verifyInsertHelper(seenMap, appleNamesToQuantities,
                                    (InsertChange) c);
                        } else if (c.getKind().equals("delete")) {
                            verifyDeleteHelper((DeleteChange) c);
                            deleteCounts++;
                        } else if (c.getKind().equals("update")) {
                            verifyUpdateHelper(seenMap, appleNamesToQuantities,
                                    (UpdateChange) c);
                        } else {
                            throw new Exception(String.format(
                                    "Unrecognized change kind %s",
                                    c.getKind()));
                        }
                }
            }
            if (kind.equals("insert") || kind.equals("update")) {
                seenAll = true;
                for (boolean val : seenMap.values()) {
                    seenAll = seenAll && val;
                }
            } else if (kind.equals("delete")) {
                seenAll = deleteCounts == appleNamesToQuantities.size();
            }
        }
        assertTrue(seenAll);
    }

    void verifyDeleteHelper(DeleteChange dc) throws Exception {
        assertEquals(dc.getKind(), "delete");
        assertEquals(dc.getTable(), "apples");
        assertEquals(dc.getSchema(), "public");
        assertEquals(dc.getColumnnames(), Arrays.asList("id"));
    }

    void verifyInsertHelper(Map<String, Boolean> seenMap,
                                   Map<String, Integer> appleNamesToQuantities,
                                   InsertChange ic) throws Exception {
        String appleName = (String) ic.getColumnvalues().get(1);
        int seenQuantity = (int) ic.getColumnvalues().get(2);
        if (appleNamesToQuantities.containsKey(appleName) &&
                appleNamesToQuantities.get(appleName).equals(seenQuantity)) {
            seenMap.put(appleName, true);
        }
        assertEquals(ic.getKind(), "insert");
        assertEquals(ic.getTable(), "apples");
        assertEquals(ic.getSchema(), "public");
        assertEquals(ic.getColumnnames(), Arrays.asList("id", "name",
                "quantity"));
    }

    void verifyUpdateHelper(Map<String, Boolean> seenMap,
                            Map<String, Integer> appleNamesToQuantities,
                            UpdateChange uc) throws Exception {
        String appleName = (String) uc.getColumnvalues().get(1);
        int seenQuantity = (int) uc.getColumnvalues().get(2);
        if (appleNamesToQuantities.containsKey(appleName) &&
                appleNamesToQuantities.get(appleName).equals(seenQuantity)) {
            seenMap.put(appleName, true);
        }
        assertEquals(uc.getKind(), "update");
        assertEquals(uc.getTable(), "apples");
        assertEquals(uc.getSchema(), "public");
        assertEquals(uc.getColumnnames(), Arrays.asList("id", "name",
                "quantity"));
    }

    void verifyPostgresInsertRecordsAppearOnKinesisStream(
            Map<String, Integer> appleNamesToQuantities) throws Exception {
        verifyPostgresRecordsAppearOnKinesisStream(appleNamesToQuantities,
                "insert");
    }

    void verifyPostgresDeleteRecordsAppearOnKinesisStream(
            Map<String, Integer> appleNamesToQuantities) throws Exception {
        verifyPostgresRecordsAppearOnKinesisStream(appleNamesToQuantities,
                "delete");
    }

    void verifyPostgresUpdateRecordsAppearOnKinesisStream(
            Map<String, Integer> appleNamesToQuantities) throws Exception {
        verifyPostgresRecordsAppearOnKinesisStream(appleNamesToQuantities,
                "update");
    }
}



