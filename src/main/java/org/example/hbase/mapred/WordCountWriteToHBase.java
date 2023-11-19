package org.example.hbase.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * MapReduce按行读取Hbase表数据
 */
public class WordCountWriteToHBase extends Configured {
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

    //输出为byte的key和次数
    public static class WriteHBaseMapper extends Mapper<Object, Text, ImmutableBytesWritable, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, ImmutableBytesWritable, IntWritable>.Context context) throws IOException, InterruptedException {
            StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
            while (stringTokenizer.hasMoreTokens()) {
                word.set(stringTokenizer.nextToken());
                context.write(new ImmutableBytesWritable(Bytes.toBytes(word.toString())), one);
            }
        }
    }

    public static class WriteHBaseReducer extends TableReducer<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable> {
        @Override
        protected void reduce(ImmutableBytesWritable key, Iterable<IntWritable> values, Reducer<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable, Mutation>.Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            Put put = new Put(key.get());
//            put.addImmutable(Bytes.toBytes("content"), Bytes.toBytes("count"), Bytes.toBytes(sum + ""));
            put.addColumn(Bytes.toBytes("content"), Bytes.toBytes("count"), Bytes.toBytes(sum + ""));
            context.write(key, put);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String tableName = "wordcount";
        connection = ConnectionFactory.createConnection(conf);
        TableName tableName1 = TableName.valueOf(tableName);
        Table table = connection.getTable(tableName1);
        Admin admin = connection.getAdmin();
        //如果表格存在则删除
        if (admin.tableExists(tableName1)) {
            admin.disableTable(tableName1);
            admin.deleteTable(tableName1);
        }
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName1);
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("content");
        hTableDescriptor.addFamily(hColumnDescriptor);
        admin.createTable(hTableDescriptor);

        Job job = Job.getInstance(conf, "WordCountWriteToHBase");
        job.setJarByClass(WordCountWriteToHBase.class);
        job.setMapperClass(WriteHBaseMapper.class);
        TableMapReduceUtil.initTableReducerJob(tableName, WriteHBaseReducer.class, job, null, null, null, null, false);

        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Put.class);
        FileInputFormat.addInputPaths(job, args[0]);
        job.waitForCompletion(true);

    }

    /**

     读取HBase表数据，保存到hdfs里面
     执行后，看hdfs://10.211.55.4:9000/pip/hbase-reduce/02/里面的文件内容

     */
}
