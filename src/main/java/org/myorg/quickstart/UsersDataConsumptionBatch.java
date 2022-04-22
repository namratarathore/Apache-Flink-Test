package org.myorg.quickstart;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.myorg.quickstart.records.DataConsumption;
import org.myorg.quickstart.records.DataConsumptionRecordDeserializationSchema;

import java.time.Instant;
import static org.apache.flink.table.api.Expressions.*;

public class UsersDataConsumptionBatch {
    public static void main(String[] args){
        // get the input kafka topic

        /* Created a Kafka Producer job which populates the Kafka topic
        * To see the Kafka topic locally
        * ./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic input --property print.timestamp=true */
        final ParameterTool input_params = ParameterTool.fromArgs(args);
        String topic = input_params.get("topic", "input");
        String brokers = input_params.get("bootstrap.servers", "localhost:9092");

        /* configuration object which stores key/value pairs*/
        final Configuration flinkConfig = new Configuration();

      /*  The StreamExecutionEnvironment is the context in which a streaming program is executed.
          LocalStreamEnvironment,will cause execution in the current JVM
          RemoteStreamEnvironment, will cause execution on a remote setup.
          It provides methods to control the job execution (such as setting the parallelism or the fault tolerance/checkpointing parameters) and to interact with the outside world (data access).*/
        final StreamExecutionEnvironment context = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(flinkConfig);

        //running a batch app
        context.setRuntimeMode(RuntimeExecutionMode.BATCH);

        // used for executing SQL statements, but can be replaced with DataStream API
        StreamTableEnvironment table_context = StreamTableEnvironment.create(context);

        // Creating a bounded kafka source for the batch app
        // NOTE: give the record type i.e., DataConsumption when creating the data source
        KafkaSource<DataConsumption> source = KafkaSource.<DataConsumption>builder()
                                                .setBootstrapServers(brokers)
                                                .setTopics(topic)
                .setStartingOffsets(OffsetsInitializer.timestamp(
                        Instant.parse("2021-10-01T00:00:00.000Z").toEpochMilli()
                ))
                .setBounded(OffsetsInitializer.timestamp(Instant.parse("2021-10-31T23:59:59.999Z").toEpochMilli()))
                // the key will be ignored for deserialization
                .setValueOnlyDeserializer(new DataConsumptionRecordDeserializationSchema())
                .build();

        // Create the data stream
        // No watermarking needed for batch
        DataStream<DataConsumption> records = context.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // Creating a result table
        Table results = table_context.fromDataStream(records)
                                    .groupBy($("account"))
                                    .select($("account"),
                                            $("bytesUsed").sum().as("totalConsumption"),
                                            $("ts").max().as("asOf"));


        // print the results
        // sometimes port might already be taken in that case kill the other process: sudo lsof -i :8081, kill -9 29497
        results.execute().print();
    }
}
