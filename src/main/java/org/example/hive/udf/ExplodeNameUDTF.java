package org.example.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;


/**
 * 用户自定义表生成函数
 * 分割字符串
 */
@Description(
        name = "explode_name",
        value = "_FUNC_(col) - The parameter is a column name."
                + " The return value is two strings.",
        extended = "Example:\n"
                + " > SELECT _FUNC_(col) FROM src;"
                + " > SELECT _FUNC_(col) AS (name, surname) FROM src;"
                + " > SELECT adTable.name,adTable.surname"
                + " > FROM src LATERAL VIEW _FUNC_(col) adTable AS name, surname;"
)

public class ExplodeNameUDTF extends GenericUDTF {

    //指定输入输出参数：输入的Object Inspectors和输出的Struct
    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs)

            throws UDFArgumentException {

        if (argOIs.length != 1) {
            throw new UDFArgumentException("ExplodeStringUDTF takes exactly one argument.");
        }

        if (argOIs[0].getCategory() != ObjectInspector.Category.PRIMITIVE
                && ((PrimitiveObjectInspector) argOIs[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentTypeException(0, "ExplodeStringUDTF takes a string as a parameter.");
        }

        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

        fieldNames.add("name");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("surname");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        //输出的字段和结构
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);

    }

    //处理输入记录，然后通过forward()方法返回输出结果
    @Override
    public void process(Object[] args) throws HiveException {

        String input = args[0].toString();

        String[] name = input.split(" ");

        if (name.length == 1) {
            name = findLastUpperLetterAndSplit(input);
        }

        forward(name);

    }

    //把MarySmith转换成["Mary", "Smith"]
    public static String[] findLastUpperLetterAndSplit(String input) {
        StringBuffer stringBuffer = new StringBuffer(input);
        int length = input.length();
        for (int i = 0; i < length; i++) {
            char c = stringBuffer.charAt(length - i - 1);
            if (c <= 'Z' && c >= 'A') {
                String[] ret = new String[2];
                ret[0] = input.substring(0, length - i - 1);
                ret[1] = input.substring(length - i - 1);
                return ret;
            }
        }
        return new String[]{input, "dummy"};
    }


    //通知UDTF没有行可以处理，可以在该方法中清理代码或者附加其他处理输出
    @Override
    public void close() throws HiveException {

    }

}
