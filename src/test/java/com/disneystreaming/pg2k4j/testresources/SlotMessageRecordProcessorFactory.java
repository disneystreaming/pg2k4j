package com.disneystreaming.pg2k4j.testresources;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker
        .InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.metrics.interfaces.MetricsLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SlotMessageRecordProcessorFactory
        implements IRecordProcessorFactory, Runnable {

    private static final Logger logger =
            LoggerFactory.getLogger(SlotMessageRecordProcessor.class);

    public SlotMessageRecordProcessor slotMessageRecordProcessor = null;

    private KinesisClientLibConfiguration kinesisClientLibConfiguration;


    public SlotMessageRecordProcessorFactory(final String dynamoEndpoint,
                                             final String kinesisEndpoint,
                                             final String streamName) {
        AWSCredentialsProvider awsCredentialsProvider =
                new AWSStaticCredentialsProvider(
                        new BasicAWSCredentials(
                                "test", "test"));
        this.kinesisClientLibConfiguration =
                new KinesisClientLibConfiguration("test",
                        streamName, awsCredentialsProvider, "test")
                        .withMaxRecords(1000)
                        .withCallProcessRecordsEvenForEmptyRecordList(false)
                        .withDynamoDBEndpoint(dynamoEndpoint)
                        .withKinesisEndpoint(kinesisEndpoint)
                        .withMetricsLevel(MetricsLevel.NONE)
                        .withInitialPositionInStream(
                                InitialPositionInStream.TRIM_HORIZON);
    }

    @Override
    public IRecordProcessor createProcessor() {
        this.slotMessageRecordProcessor = new SlotMessageRecordProcessor();
        return this.slotMessageRecordProcessor;
    }


    @Override
    public void run() {
        final Worker worker = new Worker.Builder()
                .recordProcessorFactory(this)
                .config(this.kinesisClientLibConfiguration)
                .build();
        worker.run();
    }

    public void waitForConsumer() throws Exception {
        while (slotMessageRecordProcessor == null ||
                !slotMessageRecordProcessor.initialized) {
            logger.info("KCL not initialized yet, sleeping then retrying");
            Thread.sleep(1000);
        }
    }
}
