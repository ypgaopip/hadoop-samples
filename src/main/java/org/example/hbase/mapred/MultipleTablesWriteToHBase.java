package org.example.hbase.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * 使用MapReduce同时写入多张表
 */
public class MultipleTablesWriteToHBase {
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

    public static class WriteBaseMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, ImmutableBytesWritable, Put>.Context context) throws IOException, InterruptedException {
            String[] info = value.toString().split(",", -1);
            String name = info[0];
            String chinese = info[1];
            String english = info[2];
            String math = info[3];

            ImmutableBytesWritable putTable = new ImmutableBytesWritable("chinese".getBytes());
            Put put = new Put(name.getBytes());
            put.addColumn("info".getBytes(), "grade".getBytes(), chinese.getBytes());
            context.write(putTable, put);

            putTable = new ImmutableBytesWritable("english".getBytes());
            put = new Put(name.getBytes());
            put.addColumn("info".getBytes(), "grade".getBytes(), english.getBytes());
            context.write(putTable, put);

            putTable = new ImmutableBytesWritable("math".getBytes());
            put = new Put(name.getBytes());
            put.addColumn("info".getBytes(), "grade".getBytes(), math.getBytes());
            context.write(putTable, put);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        {
            String tableNameStr = "chinese";
            TableName tableName = TableName.valueOf(tableNameStr);
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
            ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.of("info");
            TableDescriptor tableDescriptor = tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor).build();
            if (!admin.tableExists(tableName))
                admin.createTable(tableDescriptor);
        }

        {
            String tableNameStr = "english";
            TableName tableName = TableName.valueOf(tableNameStr);
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
            ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.of("info");
            TableDescriptor tableDescriptor = tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor).build();
            if (!admin.tableExists(tableName))
                admin.createTable(tableDescriptor);
        }

        {
            String tableNameStr = "math";
            TableName tableName = TableName.valueOf(tableNameStr);
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
            ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.of("info");
            TableDescriptor tableDescriptor = tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor).build();
            if (!admin.tableExists(tableName))
                admin.createTable(tableDescriptor);
        }

        Job job = Job.getInstance(conf, "MultipleTablesWriteToHBase");
        job.setJarByClass(MultipleTablesWriteToHBase.class);
        job.setMapperClass(WriteBaseMapper.class);

        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        FileInputFormat.addInputPaths(job, args[0]);

        //输出格式为MultiTableHFileOutputFormat
        //哎，这个地方因为把class名字写错了，折腾了老半天，写成了MultiTableHFileOutputFormat.class
        job.setOutputFormatClass(MultiTableOutputFormat.class);

        job.setNumReduceTasks(0);
        job.waitForCompletion(true);


    }
    /*


    result:

    scan 'chinese'
ROW                                         COLUMN+CELL
 john                                       column=info:grade, timestamp=2023-11-19T07:44:30.463, value=76
 lily                                       column=info:grade, timestamp=2023-11-19T07:44:30.463, value=87
 linda                                      column=info:grade, timestamp=2023-11-19T07:44:30.463, value=66
 lucy                                       column=info:grade, timestamp=2023-11-19T07:44:30.463, value=90
 rubby                                      column=info:grade, timestamp=2023-11-19T07:44:30.463, value=87
 smith                                      column=info:grade, timestamp=2023-11-19T07:44:30.463, value=77
6 row(s)

scan 'english'
ROW                                         COLUMN+CELL
 john                                       column=info:grade, timestamp=2023-11-19T07:44:30.593, value=77
 lily                                       column=info:grade, timestamp=2023-11-19T07:44:30.593, value=76
 linda                                      column=info:grade, timestamp=2023-11-19T07:44:30.593, value=65
 lucy                                       column=info:grade, timestamp=2023-11-19T07:44:30.593, value=76
 rubby                                      column=info:grade, timestamp=2023-11-19T07:44:30.593, value=88
 smith                                      column=info:grade, timestamp=2023-11-19T07:44:30.593, value=76

  scan 'math'
ROW                                         COLUMN+CELL
 john                                       column=info:grade, timestamp=2023-11-19T07:44:30.606, value=65
 lily                                       column=info:grade, timestamp=2023-11-19T07:44:30.606, value=65
 linda                                      column=info:grade, timestamp=2023-11-19T07:44:30.606, value=64
 lucy                                       column=info:grade, timestamp=2023-11-19T07:44:30.606, value=77
 rubby                                      column=info:grade, timestamp=2023-11-19T07:44:30.606, value=98
 smith                                      column=info:grade, timestamp=2023-11-19T07:44:30.606, value=88
6 row(s)

     */

}
