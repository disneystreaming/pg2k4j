package com.disneystreaming.pg2k4j.testresources;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.Record;
import com.disneystreaming.pg2k4j.models.SlotMessage;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SlotMessageRecordProcessor implements IRecordProcessor {

    private static final Logger logger = 
            LoggerFactory.getLogger(SlotMessageRecordProcessor.class);

    public boolean initialized = false;

    private final ObjectReader recordReader;
    private final ObjectReader recordsReader;
    public List<SlotMessage> slotMessages = new ArrayList<>();

    public SlotMessageRecordProcessor() {
        ObjectMapper objectMapper = new ObjectMapper();
        this.recordReader = objectMapper.reader().forType(SlotMessage.class);
        this.recordsReader = objectMapper.reader().forType(SlotMessage[].class);
    }

    public void processRecords(final List<SlotMessage> records) {
        this.slotMessages.addAll(records);
    }

    @Override
    public void processRecords(final ProcessRecordsInput processRecordsInput) {
        final List<SlotMessage> records = getRecords(processRecordsInput);
        logger.debug("Processing {} records", records.size());
        processRecords(records);
    }

    @Override
    public void initialize(final InitializationInput initializationInput) {
        this.initialized = true;
    }

    @Override
    public void shutdown(final ShutdownInput shutdownInput) {
        if (shutdownInput.getShutdownReason() == ShutdownReason.TERMINATE) {
            try {
                shutdownInput.getCheckpointer().checkpoint();
            } catch(Exception e) {
                logger.error("Error checkpointing", e);
            }
        }
    }

    private List<SlotMessage> getRecords(final ProcessRecordsInput
                                                 processRecordsInput) {
        final List<SlotMessage> records = new ArrayList<>();

        for (final Record record : processRecordsInput.getRecords()) {
            final byte[] incomingData = record.getData().array();
            logger.info("Marshalling Record : {}", new String(incomingData,
                    StandardCharsets.UTF_8));
            try {
                try {
                    records.addAll(Arrays.asList(this.recordsReader
                            .readValue(incomingData)));
                } catch (final JsonMappingException jme) {
                    logger.info("Found single incoming Kinesis record - "
                           + "attempting to deserialize as a single Record");

                    records.add(this.recordReader.readValue(incomingData));
                }
            } catch (final Exception e) {
                logger.error("RecordProcessor/Record type mismatch", e);
            }
        }
        return records;
    }

}
