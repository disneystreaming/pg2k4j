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

 *******************************************************************************/

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