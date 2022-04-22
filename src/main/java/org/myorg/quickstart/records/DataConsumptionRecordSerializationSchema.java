package org.myorg.quickstart.records;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

public class DataConsumptionRecordSerializationSchema implements KafkaRecordSerializationSchema<DataConsumption> {

    private static final long serialVersionUID = 1L;

    private String topic;
    private static final ObjectMapper objectMapper =
            JsonMapper.builder()
            .build()
            .registerModule(new JavaTimeModule())
            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

    public  DataConsumptionRecordSerializationSchema() {}

    public DataConsumptionRecordSerializationSchema(String topic){
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(DataConsumption record, KafkaSinkContext context, Long timestamp){
        try{
            return new ProducerRecord<>(topic, null, record.ts.toEpochMilli(), null,  objectMapper.writeValueAsBytes(record));
        } catch(JsonProcessingException e){
            throw new IllegalArgumentException("Could not serialize record" + record, e);
        }
    }


}
