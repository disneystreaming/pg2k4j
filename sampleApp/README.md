# Run Sample App

This tutorial will show you how to run a pg2k4j configured against a real AWS Kinesis stream and an AWS RDS Postgres instance.

Note that it's also possible to run pg2k4j against a localstack Kinesis stream by setting
the value of `KINESIS_ENDPOINT_URL` to a value like `localstack:4568`. It's also possible to run pg2k4j against
a locally running Postgres instance, by mirroring the configuration parameters we set on our RDS postgres instance. Unfortunately
it's impossible to run pg2k4j against an AWS RDS Aurora Postgres instance, because Aurora Postgres does not have a WAL.

## Build Jar

Run the below command which will output a file called ```target/sampleApp-1.0-SNAPSHOT-jar-with-dependencies.jar```

```bash
mvn clean install
```

## Create Kinesis Stream

In AWS console navigate to Kinesis, and create a stream called `pg2k4j-sampleApp` in region `us-east-1`.

![Create Kinesis Stream](https://github.bamtech.co/personalization/pg2k4j/blob/master/sampleApp/sampleAppKinesisStream.png)

## Start AWS RDS Postgres

### Create a Parameter Group

In AWS console navigate to RDS then parameter groups and create a parameter group for the `postgres10` family.

![Create Parameter Group](https://github.bamtech.co/personalization/pg2k4j/blob/master/sampleApp/sampleAppParameterGroup.png)

In this parameter group, set the following values:

```bash
rds.logical_replication 1
max_wal_senders 10
max_replication_slots 10
```

### Launch Instance

In AWS console navigate to RDS then instances and select `Launch Instance`. Follow the creation wizard,
selecting `Postgresql` for the DB engine and `Postgresql 10.3-R1` for the DB engine version.

As shown below, associate the parameter group you created in the previous step with this instance.

![Associate Parameter Group](https://github.bamtech.co/personalization/pg2k4j/blob/master/sampleApp/associateParameterGroup.png)

## Run Jar

Modify the values in [config.yaml](https://github.bamtech.co/personalization/pg2k4j/blob/master/sampleApp/src/main/resources/config.yaml)
to accurately reflect the parameters used to launch the kinesis stream and postgres instance.

Run the following command. `AWS_CONFIG_FILE` is a file that looks like the following

```shell
# Optional Profile
[profile myprofile]
role_arn = arn:aws:iam::000000000000:role/myrole
source_profile = default
role_session_name = myrole

[default]
output = json
region = us-east-1
aws_access_key_id = XXXXXXXXXXXXXXXXXXXX
aws_secret_access_key = xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

If you don't have any profiles configured, or want to run this command without a profile, omit the `AWS_PROFILE` setting.

```
CONFIG_PATH=file://${PWD}/src/main/resources/config.yaml AWS_PROFILE=myprofile AWS_CONFIG_FILE=/path/to/your/aws/creds/and/conf java -jar target/sampleApp-1.0-SNAPSHOT-jar-with-dependencies.jar
``` 

You should see the following messages when the app starts up

```bash
2018-07-17T13:13:34.833 DEBUG [com.disney.pg2k4j.PostgresConnector] - Connected to postgres
2018-07-17T13:13:34.834 INFO  [com.disney.pg2k4j.PostgresConnector] - Attempting to create replication slot sampleAppSlot
2018-07-17T13:13:35.125 INFO  [com.disney.pg2k4j.PostgresConnector] - Created replication slot
2018-07-17T13:13:35.591 INFO  [com.disney.pg2k4j.SlotReaderKinesisWriter] - Consuming from slot sampleAppSlot
```

You should be able to run the following query on postgres and get the following result

```sql
select * from pg_replication_slots

1	sampleappslot	wal2json	logical	16444	sampleApp	f	t	114618	null	5736302	30A/58000060	30A/58000098
```

Now, to see this work create a table

```sql
CREATE TABLE sampleapptable(uid VARCHAR)
insert into sampleapptable values('uid') 
```

This results in the following logs indicated we've successfully published to Kinesis

```bash
2018-07-17T14:21:13.252 TRACE [com.disney.pg2k4j.SlotReaderKinesisWriter] - Writing record with data {"xid":5736364,"change":[{"kind":"insert","columnnames":["uid"],"columntypes":["character varying"],"table":"sampleapptable","columnvalues":["uid"],"schema":"public"}]} to stream
```
