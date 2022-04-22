package org.myorg.quickstart.sources;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.myorg.quickstart.records.DataConsumption;

import java.time.Instant;
import java.util.Random;

public class DataConsumptionRecordGenerator extends RichParallelSourceFunction<DataConsumption> {

    private static final long serialVersionUID = 1L;

    public static final int NUMBER_OF_ACCOUNTS_PER_INSTANCE = 10000;
    public static final int EVENTS_PER_DAY_PER_ACCOUNT = 4;
    public static final int MILLISECONDS_PER_DAY = 86_400_000;
    public static final long DELTA_T =
            MILLISECONDS_PER_DAY / (NUMBER_OF_ACCOUNTS_PER_INSTANCE * EVENTS_PER_DAY_PER_ACCOUNT);
    public static final Instant BEGINNING = Instant.parse("2021-10-01T00:00:00.00Z");
    public static final Instant SLOWDOWN = Instant.parse("2021-10-26T17:00:00.00Z");

    private volatile boolean running = true;
    private int indexOfThisSubtask;

    @Override
    public void run(SourceContext<DataConsumption> ctx) throws Exception {

        this.indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
        DataConsumptionRecordIterator dataConsumptionRecordIterator = new DataConsumptionRecordIterator(indexOfThisSubtask);

        while (running) {
            DataConsumption event = dataConsumptionRecordIterator.next();
            ctx.collect(event);
            // slow down once the timestamps catch up to the date of Flink Forward
            if (event.ts.compareTo(SLOWDOWN) > 0) {
                Thread.sleep(DELTA_T);
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    static class DataConsumptionRecordIterator {

        private long nextTimestamp;
        private Random random;
        private int indexOfThisSubtask;

        public DataConsumptionRecordIterator(int indexOfThisSubtask) {
            nextTimestamp = BEGINNING.toEpochMilli();
            random = new Random();
            this.indexOfThisSubtask = indexOfThisSubtask;
        }

        public DataConsumption next() {
            Instant ts = Instant.ofEpochMilli(nextTimestamp);
            nextTimestamp += DELTA_T;
            String account = nextAccountForInstance();

            int bytesUsed = 18 * random.nextInt(10_000_000);
            return new DataConsumption(ts, account, bytesUsed);
        }

        private String nextAccountForInstance() {
            return DataConsumption.accountForSubtaskAndIndex(
                    indexOfThisSubtask, random.nextInt(NUMBER_OF_ACCOUNTS_PER_INSTANCE));
        }
    }
}
