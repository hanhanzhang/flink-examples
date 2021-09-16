package com.sdu.flink.sql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class FlinkSqlUtils {

    private FlinkSqlUtils() {

    }

    public static TableEnvironment getTableEnvironment() {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        return TableEnvironment.create(settings);
    }

}
