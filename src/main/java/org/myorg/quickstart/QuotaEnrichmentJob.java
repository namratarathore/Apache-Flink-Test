package org.myorg.quickstart;

import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.myorg.quickstart.sources.AccountUpdateGenerator;

public class QuotaEnrichmentJob {
    public static void main(String [] args) throws Exception{
        // set up the env
        final Configuration flinkConfig = new Configuration();
        final StreamExecutionEnvironment context =  StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(flinkConfig);

        context.setParallelism(4);

        StreamTableEnvironment tableContext = StreamTableEnvironment.create(context);

        // create table for consumption, using Kafka Connector( look at the Kafka Producer Job)
        tableContext.executeSql(String.join(
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
                ")"
        ));

        /*create a stream for accounts using the AccountUpdateGenerator class
        create a table schema
        convert the stream into table*/

        // add the source as the AccountUpdateGenerator class
        DataStream<Row> accountUpdateStream = context.addSource(new AccountUpdateGenerator())
                                                .returns(AccountUpdateGenerator.typeProduced());

        // create a table for account updates
        Schema accountUpdateSchema = Schema.newBuilder()
                .column("id", "STRING NOT NULL")
                .column("quota", "BIGINT")
                .column("ts", "TIMESTAMP_LTZ(3)")
                .watermark("ts", "SOURCE_WATERMARK()")
                .primaryKey("id")
                .build();
        Table accountUpdates = tableContext.fromChangelogStream(accountUpdateStream, accountUpdateSchema);
        tableContext.createTemporaryView("account", accountUpdates);

        /* Join the data consumption table with the account table */
        // Account table consists of Quota (data) for each account_id till that timestamp
        Table joinedRecords = tableContext.sqlQuery(String.join(
                "\n",
                "SELECT data_consumption.account, data_consumption.bytesUsed, account.quota, data_consumption.ts",
                "FROM data_consumption JOIN account FOR SYSTEM_TIME AS OF data_consumption.ts",
                "ON data_consumption.account = account.id",
                "ORDER BY data_consumption.ts"
        ));

        joinedRecords.execute().print();


    }
}
