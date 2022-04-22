
Reference : https://www.youtube.com/watch?v=RCP9-HdId9w

Import as a Maven Project
OR
Create your own maven project 
## How to create a new maven project for your own Flink application

```bash
curl https://flink.apache.org/q/quickstart.sh | bash -s 1.14.3
```

First use docker-compose to start up a Kafka cluster:
```
docker-compose up -d
```

Running `KafkaProducerJob` in your IDE will populate an _input_ topic with DataConsumption Records.

After that any main class can be executed which expects to read from this input Kafka topic.

Flink UI : http://localhost:8081

 ### UsersDataConsumptionBatch
Data Consumption for a particular account with timestamp in a batch env with bounded stream

 ### UsersDataConsumptionStreaming
Data Consumption for a particular account with timestamp in a streaming env with unbounded stream

### QuotaEnrichmentsJob
Populates the quota/limit for data for account

### Alerting Job
Alerts the accounts closer to their quotas 

### Setting up a Flink cluster

```bash
curl -OL https://dlcdn.apache.org/flink/flink-1.14.3/flink-1.14.3-bin-scala_2.12.tgz
tar -xzf flink-*.tgz
cd flink-1.14.3
ls -la bin/
./bin/start-cluster.sh
```

Then browse to http://localhost:8081.

To see the cluster run the streaming WordCount example:

```bash
./bin/flink run examples/streaming/WordCount.jar
tail log/flink-*-taskexecutor-*.out
```

## How to run SQL
Spin up the flink cluster again and spin up the sql-client using the command:
``` bash
bin/sql-client.sh
```

### Advantages:
1. Robust State management
2. Event time processing
3. Easy to switch between Batch and Streaming 

