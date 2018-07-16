package com.disney.pg2k4j;

import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;

public interface KinesisProducerConfigurationFactory {

    public KinesisProducerConfiguration getKinesisProducerConfiguration();
}
