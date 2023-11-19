package org.example.hbase.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

/**
 * 使用MapReduce读取HBase表删除特定条件的数据
 */
public class DropRowsInTableDemo {

    static Configuration conf = null;
    static Connection connection = null;

    static {
        System.setProperty("HADOOP_USER_NAME", "parallels");
        conf = HBaseConfiguration.create();
        conf.set("hbase.rootdir", "hdfs://10.211.55.4:9000/hbase");
        conf.set("hbase.master", "hdfs://10.211.55.4:16010");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        // 必须使用名称，不能够用ip
        conf.set("hbase.zookeeper.quorum", "ip-10-211-55-4,ip-10-211-55-5,ip-10-211-55-6");
    }

    // key 和value为啥类型都可以，因为没有context.write处理
    public static class ReadHBaseMapper extends TableMapper<Text, Text> {
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context) throws IOException, InterruptedException {
            System.out.println("--------------------------------------------------");
            String tableName = context.getConfiguration().get("tableName");
            //批量构建删除
            ArrayList<Delete> deletes = new ArrayList<>();
            for (Cell cell : value.rawCells()) {
                //这一行是删除条件，要对上
                Delete delete = new Delete(CellUtil.cloneRow(cell)); //使用CellUtil.cloneRow(cell),而不是cell.getRowArray()
                //这一行也是删除条件，要对上
                delete.addColumn(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell), cell.getTimestamp());//使用CellUtil.clone的方法
                deletes.add(delete);
                System.out.println("delete-- row:" + Bytes.toString(CellUtil.cloneRow(cell)) +
                        ",family:" + Bytes.toString(CellUtil.cloneFamily(cell)) +
                        ",qualifier:" + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                        ",timestamp:" + cell.getTimestamp()
                );
            }

            //批量删除
            Table table = connection.getTable(TableName.valueOf(tableName));
            table.delete(deletes);
            table.close();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        connection = ConnectionFactory.createConnection(conf);
        String tableName = "stu7";
//        String timestamp = "1111"; //to change everytime
        conf.set("tableName", tableName);

        Job job = Job.getInstance(conf, "DropRowsInTable");
        job.setJarByClass(DropRowsInTableDemo.class);

        Scan scan = new Scan();
        scan.setCaching(500); //1是Scan中的默认值，这将对MapReduce作业不利
        scan.setCacheBlocks(false); //不要将MR作业设置成true
        // 指定行键进行删除
        String deleteRowKey = "rk001";
        scan.setFilter(new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(deleteRowKey))));
//        scan.setTimestamp(new Long(timestamp));

        TableMapReduceUtil.initTableMapperJob(tableName, scan, ReadHBaseMapper.class, Text.class, Text.class, job);

        job.setOutputFormatClass(NullOutputFormat.class); //不从mapper中发送如何东西
        job.waitForCompletion(true);


    }

    /*
    删除前：
     scan 'stu7'
ROW                                         COLUMN+CELL
 rk001                                      column=info:age, timestamp=2023-11-19T06:31:39.671, value=16
 rk001                                      column=info:name, timestamp=2023-11-19T06:31:39.671, value=lucy
 rk002                                      column=info:age, timestamp=2023-11-19T06:31:39.671, value=18
 rk002                                      column=info:name, timestamp=2023-11-19T06:31:39.671, value=lily
 rk003                                      column=info:age, timestamp=2023-11-19T06:31:39.671, value=12
 rk003                                      column=info:name, timestamp=2023-11-19T06:31:39.672, value=xiaoming
3 row(s)

 // 指定行键进行删除 rk001


    输出
--------------------------------------------------
delete-- row:rk001,family:info,qualifier:age,timestamp:1700375499671
delete-- row:rk001,family:info,qualifier:name,timestamp:1700375499671

    执行后，再次查询hbase，rk001被删除
     scan 'stu7'
ROW                                         COLUMN+CELL
 rk002                                      column=info:age, timestamp=2023-11-19T06:31:39.671, value=18
 rk002                                      column=info:name, timestamp=2023-11-19T06:31:39.671, value=lily
 rk003                                      column=info:age, timestamp=2023-11-19T06:31:39.671, value=12
 rk003                                      column=info:name, timestamp=2023-11-19T06:31:39.672, value=xiaoming
2 row(s)

注意点：
    删除条件要写对
     */
}
