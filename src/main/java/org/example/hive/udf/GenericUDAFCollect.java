package org.example.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * 查询结果列转行输出
 */
@Description(

        name = "collect",
        value = "_FUNC_(col) - The parameter is a column name. "
                + "The return value is a set of the column.",
        extended = "Example:\n"
                + " > SELECT _FUNC_(col) from src;"
)

/**
 * 用户自定义聚合函数，接收从零行到多行的零个到多个列，然后返回单一值，如sum(),count()
 * UADF的Enum GenericUDAFEvaluator.Mode。Mode有4中情况：
 * 1. PARTIAL1：Mapper阶段。从原始数据到部分聚合，会调用iterate()和terminatePartial()。
 * 2. PARTIAL2：Combiner阶段，在Mapper端合并Mapper的结果数据。从部分聚合到部分聚合，会调用merge()和terminatePartial()。
 * 3. FINAL：Reducer阶段。从部分聚合数据到完全聚合，会调用merge()和terminate()。
 * 4. COMPLETE：出现这个阶段，表示MapReduce中只用Mapper没有Reducer，所以Mapper端直接输出结果了。从原始数据到完全聚合，会调用iterate()和terminate()。
 */
public class GenericUDAFCollect extends AbstractGenericUDAFResolver {

    public GenericUDAFCollect() {

    }

    //检查输入参数，指定使用哪一个resolver
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
            throws SemanticException {
        //在使用函数时只能指定一个参数
        if (parameters.length != 1) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "Exactly one argument is expected.");
        }

        //判断参数的数据类型是否为Hive的基本数据类型
        if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0,
                    "Only primitive type arguments are accepted but "
                            + parameters[0].getTypeName() + " was passed as parameter 1.");
        }
        return new GenericUDAFCollectEvaluator();
    }

    //具体逻辑处理
    @SuppressWarnings("deprecation")
    public static class GenericUDAFCollectEvaluator extends GenericUDAFEvaluator {
        private PrimitiveObjectInspector inputOI;
        private StandardListObjectInspector internalMergeOI;
        private StandardListObjectInspector loi;

        //（1）初始化UDAF
        //指定输入输出数据的类型，
        //可以看到 Mode.PARTIAL1 || m == Mode.COMPLETE 时候输入是 string
        //可以看到 Mode.PARTIAL2 || m == Mode.FINAL 时候输入是 List
        //可以看到 输出 不管什么时候都是List ，getStandardListObjectInspector，
        //  和terminatePartial和terminate这两个方法的返回值类型是一致的
        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters)
                throws HiveException {
            super.init(m, parameters);
            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                inputOI = (PrimitiveObjectInspector) parameters[0];
                return ObjectInspectorFactory.getStandardListObjectInspector(
                        (PrimitiveObjectInspector) ObjectInspectorUtils
                                .getStandardObjectInspector(inputOI));

            } else if (m == Mode.PARTIAL2 || m == Mode.FINAL) {
                internalMergeOI = (StandardListObjectInspector) parameters[0];
                inputOI = (PrimitiveObjectInspector) internalMergeOI.getListElementObjectInspector();
                loi = ObjectInspectorFactory.getStandardListObjectInspector(inputOI);
                return loi;
            }
            return null;
        }

        //定义一个buffer类型的静态类MkArrayAggregationBuffer，用于存储聚合结果
        static class ArrayAggregationBuffer implements AggregationBuffer {
            List<Object> container;
        }

        //（2）返回用于存储中间结果的对象
        // 保存数据处理的临时结果
        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            ArrayAggregationBuffer ret = new ArrayAggregationBuffer();
            reset(ret);
            return ret;
        }

        //（3）中间结果返回完成后，重置聚合
        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            ((ArrayAggregationBuffer) agg).container = new ArrayList<Object>();
        }

        //（4）处理输入记录
        //将一行新的数据载入到buffer中
        // 相当于mapper
        @Override
        public void iterate(AggregationBuffer agg, Object[] param)
                throws HiveException {
            Object p = param[0];
            if (p != null) {
                putIntoList(p, (ArrayAggregationBuffer) agg);
            }
        }

        //将一行新的数据载入到buffer中的具体实现，真正操作数据的部分。
        //将数据添加到静态类MkArrayAggregationBuffer的集合container中。
        public void putIntoList(Object param, ArrayAggregationBuffer myAgg) {
            Object pCopy = ObjectInspectorUtils.copyToStandardObject(param, this.inputOI);
            myAgg.container.add(pCopy);
        }

        //（5）处理全部输出数据中的部分数据
        //以一种持久化的方式返回当前聚合的内容
        //相当于Combiner
        //相当于Mapper端
        @Override
        public Object terminatePartial(AggregationBuffer agg)
                throws HiveException {
            ArrayAggregationBuffer myAgg = (ArrayAggregationBuffer) agg;
            ArrayList<Object> list = new ArrayList<Object>();
            list.addAll(myAgg.container);
            return list;
        }

        //（6）把两部分数据聚合起来
        //将terminatePartial（）方法中聚合的内容合并到当前聚合中
        //相当于reducer端
        @Override
        public void merge(AggregationBuffer agg, Object partial)
                throws HiveException {
            ArrayAggregationBuffer myAgg = (ArrayAggregationBuffer) agg;
            ArrayList<Object> partialResult = (ArrayList<Object>) this.internalMergeOI.getList(partial);
            for (Object obj : partialResult) {
                putIntoListForMerge(obj, myAgg);
            }
        }


        //将一行新的数据载入到buffer中的具体实现，真正操作数据的部分。
        //将数据添加到静态类MkArrayAggregationBuffer的集合container中。
        //和putIntoList一样，就是方法的名字不一样，为了区分来源，一个是来自iterate输入记录，一个来自terminatePartial的聚合list结果
        public void putIntoListForMerge(Object param, ArrayAggregationBuffer myAgg) {
            Object pCopy = ObjectInspectorUtils.copyToStandardObject(param, this.inputOI);
            myAgg.container.add(pCopy);
        }

        //（7）输出最终结果
        //返回最终聚合结果作为Hive的输出
        //这个地方可以看到和 terminatePartial 方法的逻辑是一样的，针对本例没有特殊的逻辑处理
        //相当于reducer端
        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            ArrayAggregationBuffer myAgg = (ArrayAggregationBuffer) agg;
            ArrayList<Object> list = new ArrayList<Object>();
            list.addAll(myAgg.container);
            return list;
        }

    }

}