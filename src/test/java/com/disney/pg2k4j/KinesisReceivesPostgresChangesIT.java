package com.disney.pg2k4j;

import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.services.kinesis.model.Record;
import com.disney.pg2k4j.containers.KinesisLocalStack;
import com.disney.pg2k4j.containers.Postgres;
import com.disney.pg2k4j.models.Change;
import com.disney.pg2k4j.models.DeleteChange;
import com.disney.pg2k4j.models.InsertChange;
import com.disney.pg2k4j.models.SlotMessage;
import com.disney.pg2k4j.models.UpdateChange;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class KinesisReceivesPostgresChangesIT {

    private static final Logger logger =
            LoggerFactory.getLogger(KinesisReceivesPostgresChangesIT.class);

    private static final Network network = Network.newNetwork();
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static Thread t;
    private static final int GET_RECORDS_ATTEMPTS = 10;

    @ClassRule
    public static Postgres postgres = new Postgres(network);

    @ClassRule
    public static KinesisLocalStack kinesisLocalStack =
            new KinesisLocalStack(network);

    @BeforeClass
    public static void init() throws InterruptedException,
            IOException, ExecutionException, SQLException {
        System.setProperty(
                SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY,
                "true");
        System.setProperty(
                SDKGlobalConfiguration.AWS_CBOR_DISABLE_SYSTEM_PROPERTY,
                "true");
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
    public void testInsertAndDeleteForwardedToStream() throws Exception {
        kinesisLocalStack.getAllRecords();
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
        kinesisLocalStack.getAllRecords();
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
            for(SlotMessage s: getAllSlotMessagesFromKinesisStream()) {
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

    List<SlotMessage> getAllSlotMessagesFromKinesisStream() throws Exception {
        List<SlotMessage> ret = new ArrayList<>();
        for (Record record : kinesisLocalStack.getAllRecords().getRecords()) {
            try {
                ret.add(objectMapper.readValue(record.getData().array(),
                        SlotMessage.class));
            } catch (JsonParseException jpe) {
                String serializedSlotMessages = new String(
                        record.getData().array());
                int start = serializedSlotMessages.indexOf("{");
                int end = serializedSlotMessages.lastIndexOf("}");
                String trimmedRecords = serializedSlotMessages
                        .substring(start, end + 1);
                String sanitizedRecords = ("["
                        + trimmedRecords
                                .replaceAll("[^A-Za-z0-9,{}:\\[\\]\"]", "")
                        + "]").replace(" ", "").replace("}{", "},{");
                com.disney.pg2k4j.models.SlotMessage[] ss =
                        objectMapper.readValue(sanitizedRecords,
                                SlotMessage[].class);
                ret.addAll(Arrays.asList(ss));
            }
        }
        return ret;
    }
}



