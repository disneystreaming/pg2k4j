# pg2k4j

## Overview
pg2k4j uses [logical decoding](https://www.postgresql.org/docs/9.4/static/logicaldecoding.html) in Postgres 9.4 or later to capture a consistent, continuous stream of events from the database and publishes them to an [AWS Kinesis](https://aws.amazon.com/kinesis/) stream in a format of your choosing.
It does this without requiring any changes to your data models, and guarantees each record is published to kinesis at least once.
It is only in rare cases of database reboots and process crashes that records may be published more than once.

Inspired by [pg2kinesis](https://github.com/handshake/pg2kinesis), this library aims to be more feature rich and performant than its python counterpart. pg2k4j 
can keep up with heavy write workloads of more than 1 million inserts and updates per minute. 
## Usage
Use pg2k4j by creating an instance of `SlotReaderKinesisWriter` and calling `runLoop` on that instance. This will automatically
provision a replication slot in the postgres database through which it may read chunks of the [WAL](https://www.postgresql.org/docs/current/static/wal-intro.html).
`SlotReaderKinesisWriter` may be subclassed and configured to fit any use case involving reading the postgres transaction log.
 By default `SlotReaderKinesisWriter` uses the [wal2json](https://github.com/eulerto/wal2json) output plugin to deserialize messages from the logical replication slot into
 `SlotMessage`s. The default implementation then writes out this class as json, and puts these bytes onto Kinesis using the
 [java KPL](https://github.com/awslabs/amazon-kinesis-producer/tree/master/java)
