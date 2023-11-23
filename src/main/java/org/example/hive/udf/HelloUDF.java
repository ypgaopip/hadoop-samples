package org.example.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

@Description(
        name = "hello",
        value = "_FUNC_(str) - from the input string "
                + "returns the value that is \"Hello $str\" ",
        extended = "Example:\n"
                + " > SELECT _FUNC_(str) FROM src;"
)

/**
 * 返回字符串加Hello
 */
public class HelloUDF extends UDF {
    public String evaluate(String str) {
        try {
            return "Hello " + str;
        } catch (Exception e) {
            e.printStackTrace();
            return "ERROR";
        }
    }
}