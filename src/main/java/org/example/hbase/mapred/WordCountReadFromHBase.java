package org.example.hbase.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;

/**
 * MapReduce对指定目录或文件中单词的出现次数进行统计，并将结果保存到指定的HBase表
 */
public class WordCountReadFromHBase extends Configured {
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

    // key 为行键，value为单元格
    public static class ReadHBaseMapper extends TableMapper<Text, Text> {
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context) throws IOException, InterruptedException {
            StringBuffer sb = new StringBuffer();
            for (Map.Entry<byte[], byte[]> entry : value.getFamilyMap("content".getBytes()).entrySet()) {
                String str = new String(entry.getValue());
                if (str != null) {
                    sb.append(str);
                }
                System.out.println(sb.toString());
                context.write(new Text(key.get()), new Text(sb.toString()));
            }
        }
    }

    //输出为 rowkey，和各个次数
    public static class ReadHBaseReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                result.set(value);
                context.write(key, result);
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String tableName = "wordcount";
        connection = ConnectionFactory.createConnection(conf);

        Job job = Job.getInstance(conf, "WordCountReadFromHBase");
        job.setJarByClass(WordCountReadFromHBase.class);
        TableMapReduceUtil.initTableMapperJob(tableName, new Scan(), ReadHBaseMapper.class, Text.class, Text.class, job);
        job.setReducerClass(ReadHBaseReducer.class);

        FileOutputFormat.setOutputPath(job, new Path(args[0]));
        job.waitForCompletion(true);

    }


    /*

     从hdfs的文件导入到hbase里面，执行后，查看hbase的表

     result:

     scan 'wordcount'
     ROW                                         COLUMN+CELL
     A                                          column=content:count, timestamp=2023-11-18T12:54:01.878, value=1
     Anyone                                     column=content:count, timestamp=2023-11-18T12:54:01.878, value=1
     Cloud,                                     column=content:count, timestamp=2023-11-18T12:54:01.878, value=1
     Gao.                                       column=content:count, timestamp=2023-11-18T12:54:01.878, value=1
     How                                        column=content:count, timestamp=2023-11-18T12:54:01.878, value=1
     I                                          column=content:count, timestamp=2023-11-18T12:54:01.878, value=2
     My                                         column=content:count, timestamp=2023-11-18T12:54:01.878, value=1
     Oracle                                     column=content:count, timestamp=2023-11-18T12:54:01.878, value=1
     ...
     ...

     */
}
