package com.disney.pg2k4j.sampleapp;

import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.disney.pg2k4j.PostgresConnector;
import com.disney.pg2k4j.SlotReaderCallback;
import com.disney.pg2k4j.SlotReaderKinesisWriter;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.google.common.util.concurrent.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.representer.Representer;

public class SampleSlotReaderKinesisWriter extends SlotReaderKinesisWriter {

    private static final Logger logger = LoggerFactory.getLogger(SampleSlotReaderKinesisWriter.class);
    private static final String configPathEnvVariable = "CONFIG_PATH";

    public static void main(String[] args) throws IOException {
        final String configPath = System.getenv(configPathEnvVariable);
        final Configuration conf = loadConfigurationFromLocalFile(configPath);
        new SampleSlotReaderKinesisWriter(conf).runLoop();
    }

    SampleSlotReaderKinesisWriter(Configuration conf) {
        super(conf, conf, conf, conf.getStreamName());
    }

    private static Configuration loadConfigurationFromLocalFile(final String configUrl) throws IOException {
        logger.info("Creating local config from {} ", configUrl);
        final Representer representer = new Representer();
        representer.getPropertyUtils().setSkipMissingProperties(true);
        final Path path = Paths.get(URI.create(configUrl));
        try (final InputStream in = Files.newInputStream(path)) {
            final ConfigurationSource configurationSource = new Yaml(representer).loadAs(in, ConfigurationSource.class);
            return new Configuration(configurationSource);
        }
    }
}