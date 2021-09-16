package com.sdu.flink.sql.function;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.Map;

@FunctionHint(
        input = @DataTypeHint(value = "INT"),
        output = @DataTypeHint(value = "Row<name STRING, age INT>")
)
public class UserInformationTable extends TableFunction<Row> {

    private static final Map<Integer, Row> USERS = new HashMap<>();

    static {
        USERS.put(1, Row.of("James", 36));
        USERS.put(2, Row.of("Wade", 38));
        USERS.put(3, Row.of("Bosh", 33));
        USERS.put(4, Row.of("Rondo", 28));
        USERS.put(5, Row.of("Ross", 25));
    }

    public void eval(int uid) {
        Row row = USERS.get(uid);
        if (row != null) {
            collect(row);
        }
    }
}
