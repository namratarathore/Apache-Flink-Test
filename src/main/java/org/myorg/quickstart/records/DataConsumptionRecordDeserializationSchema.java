package org.myorg.quickstart.records;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

import java.io.IOException;

public class DataConsumptionRecordDeserializationSchema extends AbstractDeserializationSchema<DataConsumption>{

    public static final long serialVersionUID = 1L;

    private transient ObjectMapper objectMapper;

    @Override
    public void open(InitializationContext context) throws Exception{
        super.open(context);
        objectMapper = JsonMapper.builder().build().registerModule(new JavaTimeModule());
    }

    @Override
    /* objectMapper readValue method is used to convert JSON into Java object of type DataConsumption */
    public DataConsumption deserialize(byte[] message) throws IOException{
        return objectMapper.readValue(message, DataConsumption.class);
    }

}
