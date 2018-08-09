package com.disney.pg2k4j;

import com.amazonaws.services.kinesis.producer.Attempt;
import com.amazonaws.services.kinesis.producer.UserRecordFailedException;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import org.postgresql.replication.LogSequenceNumber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SlotReaderCallback implements FutureCallback<UserRecordResult> {

    private static final Logger logger = LoggerFactory.getLogger(SlotReaderCallback.class);

    private final LogSequenceNumber lsn;
    private final PostgresConnector postgresConnector;
    private final SlotReaderKinesisWriter slotReaderKinesisWriter;

    protected SlotReaderCallback(SlotReaderKinesisWriter slotReaderKinesisWriter, PostgresConnector postgresConnector) {
        this.slotReaderKinesisWriter = slotReaderKinesisWriter;
        this.postgresConnector = postgresConnector;
        this.lsn = postgresConnector.getLastReceivedLsn();
    }

    @Override
    public void onFailure(Throwable t) {
        logger.error("Failed to put record with postgres sequence number {} onto the stream{}", lsn, t);
        if (t instanceof UserRecordFailedException) {
            final Attempt last = Iterables.getLast(((UserRecordFailedException) t).getResult().getAttempts());
            logger.error("Failed to put record. Error code '{}' : '{}'.", last.getErrorCode(), last.getErrorMessage());
        }
    }

    @Override
    public void onSuccess(UserRecordResult result) {
        logger.trace("Setting stream last applied and last flush lsn to {}", lsn);
        postgresConnector.setStreamLsn(lsn);
        slotReaderKinesisWriter.resetIdleCounter();
    }
}