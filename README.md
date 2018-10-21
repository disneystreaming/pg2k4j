
[![Build Status](https://travis-ci.com/disneystreaming/pg2k4j.svg?branch=master)](https://travis-ci.com/disneystreaming/pg2k4j) [![codecov](https://codecov.io/gh/disneystreaming/pg2k4j/branch/master/graph/badge.svg)](https://codecov.io/gh/disneystreaming/pg2k4j)

## pg2k4j

### Postgresql To Kinesis For Java

A tool for publishing inserts, updates, and deletes made to a [Postgresql](https://www.postgresql.org/) database to an [Amazon Kinesis](https://aws.amazon.com/kinesis/) Stream.
pg2k4j may be run as a stand-alone application from the command line, or used as a Java library where its functionality
can be extended and customized.

### Getting Started

First, setup your Postgres database to support [logical replication](https://www.postgresql.org/docs/10/static/logical-replication.html) and create an AWS Kinesis Stream.
 
#### Run pg2k4j as a Stand-alone Application
Download [Docker](https://www.docker.com/get-started) and login with your docker credentials to gain access to the [pg2k4j docker repository](https://hub.docker.com/r/rkass/pg2k4j/).
Then run the command below.
```
docker run -v /path/to/.aws/creds/:/aws_creds 
rkass/pg2k4j 
--awsconfiglocation=/aws_creds
--pgdatabase=<your_postgres_db> --pghost=<your_postgres_host> --pguser=<your_postgres_user> --pgpassword=<your_postgres_pw> 
--streamname=<your_kinesis_streamname>
``` 

When you observe the below log, pg2k4j is set to publish changes to Kinesis.

```
[main] INFO com.disneystreaming.pg2k4j.SlotReaderKinesisWriter - Consuming from slot pg2k4j
 ```
 
#### Use as a Java Library

pg2k4j artifacts are published to [maven central](https://mvnrepository.com/artifact/com.disneystreaming/pg2k4j)

##### Maven

```
<dependency>
    <groupId>com.disneystreaming.pg2k4j</groupId>
    <artifactId>pg2k4j</artifactId>
    <version>LATEST</version>
</dependency>
```

##### Gradle

```
compile group: 'com.disneystreaming.pg2k4j', name: 'pg2k4j', version: 'LATEST'
```

##### SBT

```
libraryDependencies += "info.pg2k4j" % "pg2k4j" % "LATEST"
```

To initialize and begin publishing database changes to Kinesis, create a [SlotReaderKinesisWriter](https://github.com/disneystreaming/pg2k4j/blob/master/src/main/java/com/disneystreaming/pg2k4j/SlotReaderKinesisWriter.java) 
and call its [runLoop](https://github.com/disneystreaming/pg2k4j/blob/master/src/main/java/com/disneystreaming/pg2k4j/SlotReaderKinesisWriter.java#L84) method.

### Why We Wrote pg2k4j

pg2k4j is an implementation of a powerful design pattern called [Change Data Capture](https://en.wikipedia.org/wiki/Change_data_capture).
By using pg2k4j, anyone can know the state of your database at any point in time by consuming from the Kinesis Stream.
At DSS we have rapidly changing datasets that many teams need to access and process in their own way. Using pg2k4j
alleviates the need to grant each tea