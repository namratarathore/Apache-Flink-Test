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
import static org.apache.flink.table.api.Expressions.*;
import java.lang.reflect.Parameter;

public class UsersDataConsumptionStreaming {

    public static void main(String[] args) throws Exception{
        final ParameterTool params = ParameterTool.fromArgs(args);
        String topic = params.get("topic", "input");
        String brokers = params.get("bootstrap.servers", "localhost:9092");

        // getting the flink streaming context
        final Configuration flinkConfig = new Configuration();
        final StreamExecutionEnvironment context = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(flinkConfig);
        //setting the execution type as Streaming
        context.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        StreamTableEnvironment table_context = StreamTableEnvironment.create(context);

        // create the unbounded kafka source (streaming)
        KafkaSource<DataConsumption> kafkaSource =
                KafkaSource.<DataConsumption>builder()
                        .setBootstrapServers(brokers)
                        .setTopics(topic)
        //which initializes the offsets to the earliest available offsets of each partition
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setValueOnlyDeserializer(new DataConsumptionRecordDeserializationSchema())
                        .build();

        // creating the data source i.e., read from the unbounded kafka source
        DataStream<DataConsumption> records = context.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // creating the table for analytics i.e., read from the datastream created above
        Table results = table_context.fromDataStream(records)
                                    .groupBy($("account"))
                                    .select($("account"),
                                            $("bytesUsed").sum().as("totalUsage"),
                                            $("ts").max().as("asOf"));

        // execute and print
        results.execute().print();

    }

}
