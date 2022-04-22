package org.myorg.quickstart.records;

import com.fasterxml.jackson.annotation.JsonFormat;
import java.time.Instant;
import java.util.Objects;

import static org.myorg.quickstart.sources.DataConsumptionRecordGenerator.NUMBER_OF_ACCOUNTS_PER_INSTANCE;

/* Flink POJO, it can be serialized by Flink PojoSerializer. Also has an empty default constructor */
//TODO: change it to include getters and setters instead
public class DataConsumption {
    @JsonFormat(
            shape = JsonFormat.Shape.STRING,
            pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSZ",
            timezone = "UTC"
    )

    public Instant ts;
    public String account;
    public long bytesUsed;

    public DataConsumption(){}

    public DataConsumption(final Instant ts, final String account, final long bytesUsed){
        this.ts = ts;
        this.account = account;
        this.bytesUsed = bytesUsed;
    }

    @Override
    public String toString(){
        return "Consumption{ ts = " + ts + ", account = " + account + ", bytes used= " + bytesUsed + "}";
    }


    @Override
    public boolean equals(Object o){
        if (this == o){ return true;}
        if (o == null || getClass() != o.getClass()){ return false;}

        DataConsumption that = (DataConsumption) o;
        return account.equals(that.account) && bytesUsed == that.bytesUsed && ts.equals(that.ts);
    }

    @Override
    public int hashCode(){ return Objects.hash(ts, account, bytesUsed);}

    public static String accountForSubtaskAndIndex(int subtask, int index){
        return String.format("%06d", (NUMBER_OF_ACCOUNTS_PER_INSTANCE * subtask) + index);
    }
}
