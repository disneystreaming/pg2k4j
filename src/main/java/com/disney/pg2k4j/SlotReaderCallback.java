package com.disney.pg2k4j;

import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.util.concurrent.FutureCallback;
import org.postgresql.replication.LogSequenceNumber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SlotReaderCallback implements FutureCallback<UserRecordResult> {

    private static final Logger logger = LoggerFactory.getLogger(SlotReaderKinesisWriter.class);

    private final LogSequenceNumber lsn;
    private final PostgresConnector postgresConnector;
    private final SlotReaderKinesisWriter slotReaderKinesisWriter;

    SlotReaderCallback(SlotReaderKinesisWriter slotReaderKinesisWriter, PostgresConnector postgresConnector) {
        this.slotReaderKinesisWriter = slotReaderKinesisWriter;
        this.postgresConnector = postgresConnector;
        this.lsn = postgresConnector.getLastReceivedLsn();
    }

    @Override
    public void onFailure(Throwable t) {
        logger.trace("Failed to put record with postgres sequence number {} onto the stream", lsn, t);
    }

    @Override
    public void onSuccess(UserRecordResult result) {
        logger.trace("Setting stream last applied and last flush lsn to {}", lsn);
        postgresConnector.setStreamLsn(lsn);
        slotReaderKinesisWriter.resetIdleCounter();
    }
}