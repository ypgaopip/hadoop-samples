package org.example.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;

@Description(
        name = "contains",
        value = "_FUNC_(array, element) - if array contains element," +
                " the returns the value that is \"true\", else \"false\" ",
        extended = "Example:\n"
                + " > SELECT _FUNC_(array, element) FROM src;"
)
//判断一个数组或列表中是否包含某个元素
public class ComplexUDFExample extends GenericUDF {
    // 0. ObjectInspector，通常以成员变量的形式被创建
    ListObjectInspector listOI;
    StringObjectInspector elementsOI;
    StringObjectInspector argOI;

    //出错时候打印出提示信息
    @Override
    public String getDisplayString(String[] arg0) {
        return "arrayContainsExample()"; // this should probably be better
    }

    //检查接受正确的参数类型和参数个数，只执行一次
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        // 1. 检查该记录是否传过来正确的参数数量
        if (arguments.length != 2) {
            throw new UDFArgumentLengthException("arrayContainsExample only takes 2 arguments: List<T>, T");
        }
        // 2. 检查该条记录是否传过来正确的参数类型
        ObjectInspector a = arguments[0];
        ObjectInspector b = arguments[1];
        if (!(a instanceof ListObjectInspector) || !(b instanceof StringObjectInspector)) {
            throw new UDFArgumentException("first argument must be a list / array, second argument must be a string");
        }

        // 3. 检查数组是否包含字符串，是否为字符串数组
        if (!(((ListObjectInspector) a).getListElementObjectInspector() instanceof StringObjectInspector)) {
            throw new UDFArgumentException("first argument must be a list of strings");
        }

        // 4. 检查通过后，将参数赋值给成员变量ObjectInspector，为了在evaluate()中使用
        this.listOI = (ListObjectInspector) a;
        this.elementsOI = (StringObjectInspector) this.listOI.getListElementObjectInspector();
        this.argOI = (StringObjectInspector) b;

        // 5. 用工厂类生成用于表示返回值的ObjectInspector
        return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
    }

    //处理真实的参数并返回最终结果
    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {

        try {
            // get the list and string from the deferred objects using the object inspectors
            int listLength = this.listOI.getListLength(arguments[0].get());
            String arg = "";

            if (arguments[1].get() instanceof LazyString) {
                LazyString lazyString = (LazyString) arguments[1].get();
                arg = argOI.getPrimitiveJavaObject(lazyString);
            } else if (arguments[1].get() instanceof Text) {
                Text text = (Text) arguments[1].get();
                arg = text.toString();
            }

            // see if our list contains the value we need
            for (int i = 0; i < listLength; i++) {
                LazyString listElement = (LazyString) this.listOI.getListElement(arguments[0].get(), i);
                String primitiveJavaObject = elementsOI.getPrimitiveJavaObject(listElement);
                if (arg.equals(primitiveJavaObject)) {
                    return new Boolean(true);
                }
            }
            return new Boolean(false);

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
    }

}
