# pg2k4j

[![Build Status](https://p13n.jenkins.us-east-1.bamgrid.net/job/personalization/job/pg2k4j/job/master/badge/icon)](https://p13n.jenkins.us-east-1.bamgrid.net/job/personalization/job/pg2k4j/job/master/)

## Overview
pg2k4j uses [logical decoding](https://www.postgresql.org/docs/9.4/static/logicaldecoding.html) to capture a continuous stream of events from a postgres database and publishes them to an [AWS Kinesis](https://aws.amazon.com/kinesis/) stream.
It does this without requiring any changes to your data models, and guarantees each record is published to kinesis at least once.
It is only in rare cases of database reboots and process crashes that records may be published more than once.

Inspired by [pg2kinesis](https://github.com/handshake/pg2kinesis), this library aims to be more feature rich and performant than its python counterpart. pg2k4j 
can keep up with heavy write workloads of more than 1 million inserts and updates per minute. 

Use pg2k4j by creating an instance of `SlotReaderKinesisWriter` and calling `runLoop` on that instance. This will automatically
provision a replication slot in the postgres database through which it reads chunks of the [WAL](https://www.postgresql.org/docs/current/static/wal-intro.html).
`SlotReaderKinesisWriter` may be subclassed and configured to fit any use case involving reading the postgres transaction log.
By default `SlotReaderKinesisWriter` uses the [wal2json](https://github.com/eulerto/wal2json) output plugin to deserialize messages from the logical replication slot into
`SlotMessage`s. The default implementation then writes out this class as json, and puts these bytes onto Kinesis using the
[java KPL](https://github.com/awslabs/amazon-kinesis-producer/tree/master/java).
 
When the KPL alerts the registered callback that it has successfully written to the stream, the registered callback will advance the LSN of the slot appropriately,
freeing up disk space in the postgres instance. The LSN of a replication slot will also be advanced during periods of inactivity on its slot.
During these times, the WAL is still being populated with data, but this data is unrelated to the replication slot. 

Note that the provided default configurations have been load tested and are being used in production. 

## Using

Refer to the [sample app](https://github.bamtech.co/personalization/pg2k4j/tree/master/sampleApp) for getting started.
To include this in a current project use the following maven snippet

```
<dependency>
    <groupId>com.disney.pg2k4j</groupId>
    <artifactId>pg2k4j</artifactId>
    <version>1.0.8</version>
</dependency>
```


## Contributing

Be sure that tests pass and that any new code introduced has corresponding unit tests. Run tests with 

```bash
>> mvn clean test
Tests run: 13, Failures: 0, Errors: 0, Skipped: 0
```

Submit a pr with a description detailing what this code does, and what bug or feature it addresses. Any methods
containing substantial logic should include javadocs.