package org.example.hbase.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.List;

/**
 * MapReduce建立索引表，名字为rowkey，原先表的rowkey为索引表的值
 * lucy rowkey:index 3
 * linda rowkey:index 2
 * john rowkey:index 1
 */
public class CreateHBaseIndexDemo {


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

    // key : ImmutableBytesWritable, value: row's byte[] 和 Byte[], 也就是说value的type只要和reduce的一致就可以,都不好用
    // key : ImmutableBytesWritable, value: row's Cell, 也就是说value的type只要和reduce的一致就可以,不好用
    // key : ImmutableBytesWritable, value: row's ImmutableBytesWritable, 也就是说value的type只要和reduce的一致就可以,好用
//    public static class ReadHBaseMapper extends TableMapper<ImmutableBytesWritable, Ce[]> {
//    public static class ReadHBaseMapper extends TableMapper<ImmutableBytesWritable, Cell> {
    public static class ReadHBaseMapper extends TableMapper<ImmutableBytesWritable, ImmutableBytesWritable> {
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, ImmutableBytesWritable>.Context context) throws IOException, InterruptedException {
//        protected void map(ImmutableBytesWritable key, Result value, Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Cell>.Context context) throws IOException, InterruptedException {
//        protected void map(ImmutableBytesWritable key, Result value, Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Byte[]>.Context context) throws IOException, InterruptedException {
            System.out.println("--------------------");
            //get cell, equals hbase's table's search result
            List<Cell> cells = value.listCells();
            for (Cell cell : cells) {
                String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                System.out.println("qualifier=" + qualifier);
                if (qualifier.equals("name")) {
                    //写入reduce
                    //名字（lucy之类）为key，行键(rw001之类的）为value
//                    context.write(new ImmutableBytesWritable(CellUtil.cloneValue(cell)), byte2Wrap( CellUtil.cloneRow(cell)));
//                    context.write(new ImmutableBytesWritable(CellUtil.cloneValue(cell)), cell);
                    context.write(new ImmutableBytesWritable(CellUtil.cloneValue(cell)), new ImmutableBytesWritable(CellUtil.cloneRow(cell)));
                }
            }
        }
    }

    public static Byte[] byte2Wrap(byte[] input) {

        Byte[] ret = new Byte[input.length];
        for (int i = 0; i < input.length; i++) {
            ret[i] = input[i];
        }
        return ret;
    }

    public static byte[] byte2UnWrap(Byte[] input) {

        byte[] ret = new byte[input.length];
        for (int i = 0; i < input.length; i++) {
            ret[i] = input[i];
        }
        return ret;
    }

    //    public static class ReadHBaseReduce extends TableReducer<ImmutableBytesWritable, Byte[], ImmutableBytesWritable> {
//    public static class ReadHBaseReduce extends TableReducer<ImmutableBytesWritable, Cell, ImmutableBytesWritable> {
    public static class ReadHBaseReduce extends TableReducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {
        @Override
//        protected void reduce(ImmutableBytesWritable key, Iterable<Byte[]> values, Reducer<ImmutableBytesWritable, Byte[], ImmutableBytesWritable, Mutation>.Context context) throws IOException, InterruptedException {
//        protected void reduce(ImmutableBytesWritable key, Iterable<Cell> values, Reducer<ImmutableBytesWritable, Cell, ImmutableBytesWritable, Mutation>.Context context) throws IOException, InterruptedException {
        protected void reduce(ImmutableBytesWritable key, Iterable<ImmutableBytesWritable> values, Reducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, Mutation>.Context context) throws IOException, InterruptedException {
            Put put = new Put(key.get());
//            for (Byte[] value : values) {
//            for (Cell value : values) {
            for (ImmutableBytesWritable value : values) {
//                put.addColumn("rowkey".getBytes(), "index".getBytes(), byte2UnWrap(value));
//                put.addColumn("rowkey".getBytes(), "index".getBytes(), CellUtil.cloneRow(value));
                put.addColumn("rowkey".getBytes(), "index".getBytes(), value.get());
            }
            context.write(key, put);
        }
    }

    private static void checkTable(Configuration conf) throws IOException {
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();
        TableName tn = TableName.valueOf("stu6Index");
        if (!admin.tableExists(tn)) {
            ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.of("rowkey");
            TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tn).setColumnFamily(columnFamilyDescriptor).build();
            admin.createTable(tableDescriptor);
            System.out.println("表不存在，创建" + tn.getNameAsString() + "成功");
        }
    }


    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        checkTable(conf);

        String srcTableName = "stu6";
        String indexTableName = "stu6Index";

        Job job = Job.getInstance(conf, "Create HBase table index");
        job.setJarByClass(CreateHBaseIndexDemo.class);

        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));

//        TableMapReduceUtil.initTableMapperJob(srcTableName, scan, ReadHBaseMapper.class, ImmutableBytesWritable.class, Byte[].class, job);
//        TableMapReduceUtil.initTableMapperJob(srcTableName, scan, ReadHBaseMapper.class, ImmutableBytesWritable.class, Cell.class, job);
        TableMapReduceUtil.initTableMapperJob(srcTableName, scan, ReadHBaseMapper.class, ImmutableBytesWritable.class, ImmutableBytesWritable.class, job);
        TableMapReduceUtil.initTableReducerJob(indexTableName, ReadHBaseReduce.class, job);

        job.waitForCompletion(true);


    }

    /*

    before
    scan 'stu6'
ROW                                         COLUMN+CELL
 rw001                                      column=info:age, timestamp=2023-11-17T11:22:48.636, value=16
 rw001                                      column=info:name, timestamp=2023-11-17T11:22:48.613, value=Lucy
 rw002                                      column=info:age, timestamp=2023-11-17T11:22:48.675, value=18
 rw002                                      column=info:name, timestamp=2023-11-17T11:22:48.656, value=Linda
 rw003                                      column=info:age, timestamp=2023-11-17T11:22:49.226, value=19
 rw003                                      column=info:name, timestamp=2023-11-17T11:22:48.687, value=John


run, output:
    --------------------
qualifier=name
--------------------
qualifier=name
--------------------
qualifier=name


   查看hbase

   scan 'stu6Index'
ROW                                         COLUMN+CELL
 John                                       column=rowkey:index, timestamp=2023-11-19T08:50:39.761, value=rw003
 Linda                                      column=rowkey:index, timestamp=2023-11-19T08:50:39.761, value=rw002
 Lucy                                       column=rowkey:index, timestamp=2023-11-19T08:50:39.761, value=rw001

     */
}
