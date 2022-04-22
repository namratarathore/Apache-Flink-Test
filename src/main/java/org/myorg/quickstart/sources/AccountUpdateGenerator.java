package org.myorg.quickstart.sources;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.types.Row;
import org.myorg.quickstart.records.DataConsumption;

import java.time.Instant;

public class AccountUpdateGenerator extends RichParallelSourceFunction<Row> {
    private static final long serialVersionUID = 1L;

    private volatile boolean running = true;

    @Override
    public void run(SourceContext<Row> ctx) throws Exception {
        int subtask = getRuntimeContext().getIndexOfThisSubtask();

        for (int i = 0; i < DataConsumptionRecordGenerator.NUMBER_OF_ACCOUNTS_PER_INSTANCE; i++) {
            ctx.collect(
                    Row.of(
                            DataConsumption.accountForSubtaskAndIndex(subtask, i),
                            10_000_000_000L,
                            Instant.parse("2021-01-01T00:00:00.000Z")));
        }

        ctx.emitWatermark(Watermark.MAX_WATERMARK);

        while (running) {
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    public static TypeInformation<Row> typeProduced() {
        return Types.ROW_NAMED(
                new String[] {"id", "quota", "ts"},
                Types.STRING,
                Types.LONG,
                TypeInformation.of(Instant.class));
    }
}
