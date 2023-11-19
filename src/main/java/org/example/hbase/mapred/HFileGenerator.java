package org.example.hbase.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * MapReduce生成HFile文件
 */
public class HFileGenerator {

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
        //必须设定，否则出现Mkdirs failed 错误提示
        conf.set("fs.defaultFS", "hdfs://10.211.55.4:9000");
    }

    public static class HFileMapper extends
            Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] items = line.split(",", -1);
            ImmutableBytesWritable rowkey = new ImmutableBytesWritable(
                    items[0].getBytes());

            KeyValue kv = new KeyValue(Bytes.toBytes(items[0]),
                    Bytes.toBytes(items[1]), Bytes.toBytes(items[2]),
                    System.currentTimeMillis(), Bytes.toBytes(items[3]));
            if (null != kv) {
                context.write(rowkey, kv);
            }
        }
    }

    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {
        connection = ConnectionFactory.createConnection(conf);

        Job job = new Job(conf, "HFileGenerator");
        job.setJarByClass(HFileGenerator.class);

        job.setMapperClass(HFileMapper.class);
//        job.setReducerClass(KeyValueSortReducer.class);

        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);
//        job.setPartitionerClass(SimpleTotalOrderPartitioner.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // ----- deprecated -----
//        HTableDescriptor stu7 = connection.getAdmin().getTableDescriptor(TableName.valueOf("stu7"));
//        HFileOutputFormat2.configureIncrementalLoadMap(job, stu7);
        // ----- deprecated ----- end
        Table table = connection.getTable(TableName.valueOf("stu7"));
        RegionLocator stu7RegionLocator = connection.getRegionLocator(TableName.valueOf("stu7"));
        HFileOutputFormat2.configureIncrementalLoad(job, table, stu7RegionLocator);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

/*
 * prepare:
 * <p>
 * exists 'stu7'
 * create 'stu7','info'
 * exists 'stu7'
 * <p>
 */


/*

 hfile入库hbase hbase入库方式


 一、这种方式有很多的优点：

 1. 如果我们一次性入库hbase巨量数据，处理速度慢不说，还特别占用Region资源， 一个比较高效便捷的方法就是使用 “Bulk Loading”方法，即HBase提供的HFileOutputFormat类。

 2. 它是利用hbase的数据信息按照特定格式存储在hdfs内这一原理，直接生成这种hdfs内存储的数据格式文件，然后上传至合适位置，即完成巨量数据快速入库的办法。配合mapreduce完成，高效便捷，而且不占用region资源，增添负载。

 二、这种方式也有很大的限制：

 1. 仅适合初次数据导入，即表内数据为空，或者每次入库表内都无数据的情况。

 2. HBase集群与Hadoop集群为同一集群，即HBase所基于的HDFS为生成HFile的MR的集群(额，咋表述~~~)

 生成HFile程序说明：

 ①. 最终输出结果，无论是map还是reduce，输出部分key和value的类型必须是： < ImmutableBytesWritable, KeyValue>或者< ImmutableBytesWritable, Put>。

 ②. 最终输出部分，Value类型是KeyValue 或Put，对应的Sorter分别是KeyValueSortReducer或PutSortReducer。

 ③. MR例子中job.setOutputFormatClass(HFileOutputFormat.class); HFileOutputFormat只适合一次对单列族组织成HFile文件。

 ④. MR例子中HFileOutputFormat.configureIncrementalLoad(job, table);自动对job进行配置。SimpleTotalOrderPartitioner是需要先对key进行整体排序，然后划分到每个reduce中，保证每一个reducer中的的key最小最大值区间范围，是不会有交集的。因为入库到HBase的时候，作为一个整体的Region，key是绝对有序的。

 ⑤. MR例子中最后生成HFile存储在HDFS上，输出路径下的子目录是各个列族。如果对HFile进行入库HBase，相当于move HFile到HBase的Region中，HFile子目录的列族内容没有了。

 */

/*

问题：

2023-11-19 14:19:48,663 INFO  [main] mapreduce.HFileOutputFormat2 (HFileOutputFormat2.java:configureIncrementalLoad(672)) - Configuring 1 reduce partitions to match current region count for all tables
2023-11-19 14:19:48,666 INFO  [main] mapreduce.HFileOutputFormat2 (HFileOutputFormat2.java:writePartitions(542)) - Writing partition information to /user/k/hbase-staging/partitions_89ab03d2-6ff5-4787-a90f-739ef79df854
Exception in thread "main" java.io.IOException: Mkdirs failed to create /user/k/hbase-staging (exists=false, cwd=file:/opt/sap/bigdata/hadoop-samples)
	at org.apache.hadoop.fs.ChecksumFileSystem.create(ChecksumFileSystem.java:724)
	at org.apache.hadoop.fs.ChecksumFileSystem.create(ChecksumFileSystem.java:709)

解决：

    增加下面的设定：
     conf.set("fs.defaultFS","hdfs://10.211.55.4:9000");
 */

/*

执行完，查看hdfs
hdfs dfs -ls -R /pip/hbase-reduce/03
    -rw-r--r--   3 parallels supergroup          0 2023-11-19 06:31 /pip/hbase-reduce/03/_SUCCESS
    drwxr-xr-x   - parallels supergroup          0 2023-11-19 06:31 /pip/hbase-reduce/03/info
    -rw-r--r--   3 parallels supergroup       5217 2023-11-19 06:31 /pip/hbase-reduce/03/info/27ffbc4fa8ed48a9a99fbacc6781d996

查看hbase
scan 'stu7'
    ROW                                         COLUMN+CELL
    0 row(s)
    Took 0.3029 seconds

用hbase查看（不可用）（应该是环境问题）
    hbase org.apache.hadoop.hbase.io.hfile.HFile -p -f /pip/hbase-reduce/03/info/27ffbc4fa8ed48a9a99fbacc6781d996
    2023-11-19T06:38:06,044 WARN  [main] util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
    Exception in thread "main" java.lang.NoSuchMethodError: com.codahale.metrics.MetricRegistry.histogram(Ljava/lang/String;Lcom/codahale/metrics/MetricRegistry$MetricSupplier;)Lcom/codahale/metrics/Histogram;
        at org.apache.hadoop.hbase.io.hfile.HFilePrettyPrinter$KeyValueStats.<init>(HFilePrettyPrinter.java:607)
        at org.apache.hadoop.hbase.io.hfile.HFilePrettyPrinter$KeyValueStatsCollector.<init>(HFilePrettyPrinter.java:669)
        at org.apache.hadoop.hbase.io.hfile.HFilePrettyPrinter$KeyValueStatsCollector.<init>(HFilePrettyPrinter.java:665)
        at org.apache.hadoop.hbase.io.hfile.HFilePrettyPrinter.processFile(HFilePrettyPrinter.java:314)
        at org.apache.hadoop.hbase.io.hfile.HFilePrettyPrinter.run(HFilePrettyPrinter.java:259)
        at org.apache.hadoop.util.ToolRunner.run(ToolRunner.java:76)
        at org.apache.hadoop.hbase.io.hfile.HFilePrettyPrinter.main(HFilePrettyPrinter.java:870)
        at org.apache.hadoop.hbase.io.hfile.HFile.main(HFile.java:658)



 */