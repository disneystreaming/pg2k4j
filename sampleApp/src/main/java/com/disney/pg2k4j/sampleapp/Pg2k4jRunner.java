package com.disney.pg2k4j.sampleapp;

import com.disney.pg2k4j.SlotReaderKinesisWriter;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.representer.Representer;

public class Pg2k4jRunner {

    private static final Logger logger = LoggerFactory.getLogger(Pg2k4jRunner.class);
    private static final String configPathEnvVariable = "CONFIG_PATH";

    public static void main(String[] args) throws IOException {
        final String configPath = System.getenv(configPathEnvVariable);
        final Configuration conf = loadConfigurationFromLocalFile(configPath);
        new SlotReaderKinesisWriter(conf, conf, conf, conf.getStreamName()).runLoop();
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