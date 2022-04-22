package org.myorg.quickstart;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.myorg.quickstart.records.DataConsumption;
import org.myorg.quickstart.records.DataConsumptionRecordDeserializationSchema;
import org.myorg.quickstart.records.DataConsumptionRecordSerializationSchema;
import org.myorg.quickstart.sources.DataConsumptionRecordGenerator;
import scala.util.parsing.json.JSON;

import java.util.Properties;

import static java.util.stream.Stream.builder;

public class KafkaProducerJob {

    public static void main(String[] args) throws Exception{
        /*The StreamExecutionEnvironment is the context in which a streaming program is executed.
        LocalStreamEnvironment,will cause execution in the current JVM
        RemoteStreamEnvironment, will cause execution on a remote setup.
        It provides methods to control the job execution (such as setting the parallelism or the fault tolerance/checkpointing parameters) and to interact with the outside world (data access).*/

        StreamExecutionEnvironment context = StreamExecutionEnvironment.getExecutionEnvironment();
        context.enableCheckpointing(5000L);
        context.setParallelism(4);

        // Flink config properties
        final ParameterTool params = ParameterTool.fromArgs(args);
        String topic = params.get("topic", "input");
        String brokers = params.get("bootstrap.servers", "localhost:9092");

        // setting the Kafka properties
        Properties props = new Properties();
        props.put("transaction.timeout.ms", 600000);

        // writing to kafka sink
        // Serializaing the topic data
        // While reading from this input topic, we use the Deserialization class to convert it into DataConsumption Record.
        KafkaSink<DataConsumption> sink = KafkaSink.<DataConsumption>builder()
                                            .setBootstrapServers(brokers)
                                            .setKafkaProducerConfig(props)
                                            .setRecordSerializer(new DataConsumptionRecordSerializationSchema(topic))
                                            .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                                            .setTransactionalIdPrefix("data-consumption-record-producer")
                                            .build();
        context.addSource(new DataConsumptionRecordGenerator()).sinkTo(sink);

        context.execute("KafkaProducerJob");
    }
}
