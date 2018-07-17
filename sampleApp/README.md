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
CONFIG_PATH=file://config.yaml AWS_PROFILE=myprofile AWS_CONFIG_FILE=/path/to/your/aws/creds/and/conf java -jar target/sampleApp-1.0-SNAPSHOT-jar-with-dependencies.jar
``` 