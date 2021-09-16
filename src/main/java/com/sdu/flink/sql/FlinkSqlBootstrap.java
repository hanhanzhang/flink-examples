package com.sdu.flink.sql;

import com.sdu.flink.sql.function.UserInformationTable;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class FlinkSqlBootstrap {

    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // 注册UDF
        tableEnv.createTemporaryFunction("UserInformation", UserInformationTable.class);

        // 注册TABLE
        String sourceTableStatement = "CREATE TABLE Orders (`uid` INTEGER, `orderId` BIGINT) " +
                "WITH ('connector' = 'datagen', 'rows-per-second' = '3', 'fields.uid.min' = '1', 'fields.uid.max' = '8')";
        tableEnv.executeSql(sourceTableStatement);
        String sinkTableStatement = "CREATE TABLE Results (`uid` INTEGER, name STRING, age INTEGER) " +
                "WITH ('connector' = 'print')";
        tableEnv.executeSql(sinkTableStatement);

        // DML
        String sql = "INSERT INTO Results SELECT uid, name, age FROM Orders, LATERAL TABLE(UserInformation(uid))";
        tableEnv.executeSql(sql);

    }

}
