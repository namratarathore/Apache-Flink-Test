package org.myorg.quickstart;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.myorg.quickstart.records.EnrichedUsageRecord;
import org.myorg.quickstart.sources.AccountUpdateGenerator;

public class AlertingJob {
    public static void main(String[] args) throws Exception{

            // get the parameters
            final ParameterTool params = ParameterTool.fromArgs(args);
            boolean webui = params.getBoolean("webui", true);
            StreamExecutionEnvironment context;

            // set up the env
            if (webui) {
                final Configuration flinkConfig = new Configuration();
                context = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(flinkConfig);
            } else {
                context = StreamExecutionEnvironment.getExecutionEnvironment();
            }
            context.setParallelism(4);

            StreamTableEnvironment tableContext = StreamTableEnvironment.create(context);


        // create table for consumption, using Kafka Connector( look at the Kafka Producer Job)
        // similar to QuotaEnrichmentJob
            tableContext.executeSql(
                    String.join(
                            "\n",
                            "CREATE TABLE data_consumption (",
                            "  account STRING,",
                            "  bytesUsed BIGINT,",
                            "  ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',",
                            "  WATERMARK FOR ts AS ts",
                            ") WITH (",
                            "  'connector' = 'kafka',",
                            "  'topic' = 'input',",
                            "  'properties.bootstrap.servers' = 'localhost:9092',",
                            "  'scan.startup.mode' = 'earliest-offset',",
                            "  'format' = 'json'",
                            ")"));


            /*create a stream for accounts using the AccountUpdateGenerator class
                create a table schema
                convert the stream into table*/

        DataStream<Row> accountUpdateStream =
                    context.addSource(new AccountUpdateGenerator())
                            .returns(AccountUpdateGenerator.typeProduced());


        // add the source as the AccountUpdateGenerator class
            Schema accountUpdateSchema =
                    Schema.newBuilder()
                            .column("id", "STRING NOT NULL")
                            .column("quota", "BIGINT")
                            .column("ts", "TIMESTAMP_LTZ(3)")
                            .watermark("ts", "SOURCE_WATERMARK()")
                            .primaryKey("id")
                            .build();


        // create a table for account updates
            Table accountUpdates = tableContext.fromChangelogStream(accountUpdateStream, accountUpdateSchema);

            tableContext.createTemporaryView("account", accountUpdates);

        /* Join the data consumption table with the account table */
        // Account table consists of Quota (data) for each account_id till that timestamp
            Table enrichedRecords =
                    tableContext.sqlQuery(
                            String.join(
                                    "\n",
                                    "SELECT",
                                    "  data_consumption.account AS account,",
                                    "  data_consumption.bytesUsed AS bytesUsed,",
                                    "  account.quota AS quota,",
                                    "  data_consumption.ts AS ts,",
                                    "  EXTRACT(YEAR from data_consumption.ts) AS billingYear,",
                                    "  EXTRACT(MONTH from data_consumption.ts) AS billingMonth",
                                    "FROM data_consumption JOIN account FOR SYSTEM_TIME AS OF data_consumption.ts",
                                    "ON data_consumption.account = account.id"));


           // convert the table back to the DataStream
            DataStream<EnrichedUsageRecord> enrichedStream =
                    tableContext.toDataStream(enrichedRecords, EnrichedUsageRecord.class);


          /* Grouping the data stream by month year account and quota */
            enrichedStream
                    .keyBy(EnrichedUsageRecord::keyByAccountYearMonthQuota)
                    .process(new AlertingFunction())
                    .print();


            context.execute("UsageAlertingProcessFunctionJob");
        }

        private static class AlertingFunction
                extends KeyedProcessFunction<String, EnrichedUsageRecord, String> {

            private static final long serialVersionUID = 1L;

            /* interface for reducing state. Elements can be added to the state, they will be combined using a reduce function */
            ReducingState<Long> rollingUsage;
            /* interface for partitioned single-value state. The value can be retrieved or updated. */
            ValueState<Boolean> alerted;

            @Override
            public void open(Configuration parameters) throws Exception {
                // setting up rolling and value state

                /* ReducingStateDescriptor is used to create partitioned reducing state */
                final ReducingStateDescriptor<Long> rollingUsageStateDesc =
                        new ReducingStateDescriptor<>("rolling-sum", new Sum(), Types.LONG());
                rollingUsage = getRuntimeContext().getReducingState(rollingUsageStateDesc);

                /* used to create partitioned value state */
                final ValueStateDescriptor<Boolean> alertedStateDescriptor =
                        new ValueStateDescriptor<Boolean>("alerted", Types.BOOLEAN());
                alerted = getRuntimeContext().getState(alertedStateDescriptor);

                // TODO: arrange for state to be cleared after each month
            }

            @Override
            public void processElement(EnrichedUsageRecord record, Context ctx, Collector<String> out)
                    throws Exception {

                // summing up data consumption
                rollingUsage.add(record.bytesUsed);
                long total = rollingUsage.get();


                // alerting if close to the quota
                if (alerted.value() == null && total > (0.9 * record.quota)) {
                    out.collect(
                            String.format(
                                    "WARNING: as of %s account %s has used %d out of %d bytes",
                                    record.ts, record.account, total, record.quota));

                    alerted.update(true);
                }
            }
        }

        private static class Sum implements ReduceFunction<Long> {
            private static final long serialVersionUID = 1L;

            @Override
            public Long reduce(Long value1, Long value2) throws Exception {
                return value1 + value2;
            }
        }
    }
