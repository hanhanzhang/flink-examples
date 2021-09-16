package com.sdu.flink.sql;

import static com.sdu.flink.sql.FlinkSqlUtils.getTableEnvironment;

import org.apache.flink.table.api.TableEnvironment;

public class FlinkEventTimeTemporalJoinBootstrap {

    public static void main(String[] args) {
        TableEnvironment tableEnv = getTableEnvironment();

        // 注册源表
        String sourceTableStatement = "CREATE TABLE Orders (uid INTEGER, orderId BIGINT, currencyId INTEGER, price DECIMAL(32, 2), orderTime TIMESTAMP(3), WATERMARK FOR orderTime AS orderTime ) " +
                "WITH ('connector' = 'datagen', 'rows-per-second' = '3', 'fields.currencyId.min' = '1', 'fields.currencyId.max' = '5')";
        System.out.println(sourceTableStatement);
        tableEnv.executeSql(sourceTableStatement);

        // 注册维表(必须指定主键)
        String dimensionTableStatement = "CREATE TABLE CurrencyRates (currencyId INTEGER, rate DECIMAL(32, 2), updateTime TIMESTAMP(3), WATERMARK FOR updateTime AS updateTime, PRIMARY KEY(currencyId) NOT ENFORCED) " +
                "WITH ('connector' = 'datagen', 'rows-per-second' = '3', 'fields.currencyId.min' = '1', 'fields.currencyId.max' = '5')";
        System.out.println(dimensionTableStatement);
        tableEnv.executeSql(dimensionTableStatement);

        // 注册输出表
        String sinkTableStatement = "CREATE TABLE Results (uid INTEGER, orderId BIGINT, price DECIMAL(32, 2), rate DECIMAL(32, 2), orderTime TIMESTAMP(3)) " +
                "WITH ('connector' = 'print')";
        System.out.println(sinkTableStatement);
        tableEnv.executeSql(sinkTableStatement);

        String sql = "INSERT INTO Results SELECT uid, orderId, price, rate, orderTime FROM Orders LEFT JOIN CurrencyRates FOR SYSTEM_TIME AS OF Orders.orderTime ON Orders.currencyId = CurrencyRates.currencyId";
        tableEnv.executeSql(sql);

    }

}
