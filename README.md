
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
At DSS we have rapidly changing datasets that many teams need to access and process in their own way. pg2k4j 
alleviates the need to grant database access to each team or to stand up an API on top of the dataset. This keeps the load down
on your database, making it possible to max out its write throughput. 

### Benefits Over Existing Solutions

Before writing pg2k4j, we explored existing solutions. We used [pg2kinesis](https://github.com/handshake/pg2kinesis) but found
that this implementation simply couldn't keep up with the write throughput that we required. The reason is that library is written
in Python, while pg2k4j is written in Java. Java's speed advantage over Python is due its native integration with [Amazon's
Kinesis Producer Library](https://docs.aws.amazon.com/streams/latest/dev/developing-producers-with-kpl.html). In order to 
achieve high write throughputs to Kinesis it's essential to use the KPL, and in order to use the KPL, your producer code
must be written in a JVM language so that it can interface with the KPL. This is why we wrote pg2k4j in Java and how it manages to keep
up with write throughputs of over 1 million records per minute.

### How it Works

1. pg2k4j Opens up a [Logical Replication Slot](https://www.postgresql.org/docs/10/static/logicaldecoding-explanation.html#LOGICALDECODING-REPLICATION-SLOTS) on the Postgresql database.

A replication slot will stream changes made to the database to the listener of the replication slot in the format specified
by the plugin used for that replication slot. By default pg2k4j uses the [wal2json](https://github.com/eulerto/wal2json) plugin
which outputs a json representation of a [SlotMessage][(https://github.com/disneystreaming/pg2k4j/blob/master/src/main/java/com/disneystreaming/pg2k4j/models/SlotMessage.java) to the 
listening thread. Postgres writes all data changes to the [Write Ahead Log](https://www.postgresql.org/docs/10/static/wal-intro.html), which
as well as ensuring data integrity and crash safety, make it possible to perform logical replication and for applications like
pg2k4j to exist. Each replication slot maintains a pointer to a position in the WAL, indicating the last sequence number this replication
slot has processed. This allows Postgres to flush all sections of the WAL occurring before this sequence number. Crucially, if the
application maintaining the replication slot does not update this sequence number, the storage space on the database will fill up because
Postgres won't be able to clear any sections of the WAL. To view this sequence number you can run the below query on your database.

```
select * from pg_replication_slots
```

Details of how pg2k4j manages this pointer will be outlined later in this section.

2. pg2k4j [deserializes](https://github.com/disneystreaming/pg2k4j/blob/master/src/main/java/com/disneystreaming/pg2k4j/SlotReaderKinesisWriter.java#L277) the json output sent by the wal2json plugin to a SlotMessage.

This method should be overridden when using any plugin besides wal2json as the contents from the WAL would not be json
representations of a SlotMessage.

3. pg2k4j writes this contents to the Kinesis Stream.

Done in two steps: first the SlotMessage is turned into a Stream of [UserRecord](https://github.com/awslabs/amazon-kinesis-producer/blob/master/java/amazon-kinesis-producer/src/main/java/com/amazonaws/services/kinesis/producer/UserRecord.java), and then
these UserRecords are written to the stream with a [callback attached](https://github.com/disneystreaming/pg2k4j/blob/master/src/main/java/com/disneystreaming/pg2k4j/SlotReaderKinesisWriter.java#L245) that will be invoked once the records make it to the 
stream.

4. The callback is invoked when the records succeed or fail to make it to the stream.

On a successful write to the stream pg2k4j will [advance the replication slot's sequence number](https://github.com/disneystreaming/pg2k4j/blob/master/src/main/java/com/disneystreaming/pg2k4j/SlotReaderCallback.java#L83), indicating
that any data before this point may be flushed by the database. By advancing the sequence number after receiving confirmation
that the record arrived on the stream, pg2k4j guarantees that each data change will make it to the stream. Even on database restart 
or pg2k4j restart a recovery will be made that preserves this guarantee.

There is one other scenario wherein pg2k4j will advance the sequence number. It's important to note that each Postgres instance
may have many databases, but a replication slot is configured against a single database. In the scenario where there are changes
the replication slot database is idle but the other databases are active, it's important that pg2k4j still advances its pointer into
the WAL so that Postgres doesn't hang onto these sections of the WAL. That's why pg2k4j [advances the sequence number after
a certain period of inactivity](https://github.com/disneystreaming/pg2k4j/blob/master/src/main/java/com/disneystreaming/pg2k4j/SlotReaderKinesisWriter.java#L204-L206)
which [defaults to 5 minutes](https://github.com/disneystreaming/pg2k4j/blob/master/src/main/java/com/disneystreaming/pg2k4j/ReplicationConfiguration.java#L38).

### Configuring Postgres