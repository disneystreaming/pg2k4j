/*******************************************************************************
 Copyright 2018 Disney Streaming Services

 Licensed under the Apache License, Version 2.0 (the "Apache License")
 with the following modification; you may not use this file except in
 compliance with the Apache License and the following modification to it:
 Section 6. Trademarks. is deleted and replaced with:

 6. Trademarks. This License does not grant permission to use the trade
 names, trademarks, service marks, or product names of the Licensor
 and its affiliates, except as required to comply with Section 4(c) of
 the License and to reproduce the content of the NOTICE file.

 You may obtain a copy of the Apache License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the Apache License with the above modification is
 distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied. See the Apache License for the specific
 language governing permissions and limitations under the Apache License.

 ******************************************************************************/

package com.disney.pg2k4j;

import com.amazonaws.services.kinesis.producer.Attempt;
import com.amazonaws.services.kinesis.producer.UserRecord;
import com.amazonaws.services.kinesis.producer.UserRecordFailedException;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import org.postgresql.replication.LogSequenceNumber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SlotReaderCallback implements FutureCallback<UserRecordResult> {

    private static final Logger logger =
            LoggerFactory.getLogger(SlotReaderCallback.class);

    private final LogSequenceNumber lsn;
    private final PostgresConnector postgresConnector;
    private final SlotReaderKinesisWriter slotReaderKinesisWriter;
    private final UserRecord userRecord;

    protected SlotReaderCallback(
            final SlotReaderKinesisWriter slotReaderKinesisWriterInput,
            final PostgresConnector postgresConnectorInput,
            final UserRecord userRecordInput) {
        this.slotReaderKinesisWriter = slotReaderKinesisWriterInput;
        this.postgresConnector = postgresConnectorInput;
        this.lsn = postgresConnector.getLastReceivedLsn();
        this.userRecord = userRecordInput;
    }

    @Override
    public void onFailure(final Throwable t) {
        logger.error("Failed to put record with postgres sequence number {}"
                + " onto the stream{}", lsn, t);
        if (t instanceof UserRecordFailedException) {
            final Attempt last = Iterables.getLast((
                    (UserRecordFailedException) t).getResult().getAttempts());
            logger.error("Failed to put record. Error code '{}' : '{}'.",
                    last.getErrorCode(), last.getErrorMessage());
        }
    }

    @Override
    public void onSuccess(final UserRecordResult result) {
        if (logger.isTraceEnabled()) {
            logger.trace("Setting stream last applied and last flush lsn to {}",
                    lsn);
            logger.trace("Successfully Put record on stream {} to shard {} "
                           + "with sequence number {} after {} attempts",
                    new String(userRecord.getData().array()),
                    result.getShardId(),
                    result.getSequenceNumber(),
                    result.getAttempts().size());
        }
        postgresConnector.setStreamLsn(lsn);
        slotReaderKinesisWriter.resetIdleCounter();
    }
}
