package com.sdu.flink.sql;

import static com.sdu.flink.sql.FlinkSqlUtils.getTableEnvironment;

import org.apache.flink.table.api.TableEnvironment;

import com.sdu.flink.sql.function.UserInformationTable;

public class FlinkLateralJoinBootstrap {

    public static void main(String[] args) {
        TableEnvironment tableEnv = getTableEnvironment();

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
//        String sql = "INSERT INTO Results SELECT uid, name, age FROM Orders, LATERAL TABLE(UserInformation(uid))";
        String sql = "INSERT INTO Results SELECT uid, name, age FROM Orders LEFT OUTER JOIN LATERAL TABLE(UserInformation(uid)) AS T(name, age) ON TRUE";
        tableEnv.executeSql(sql);

    }

}
