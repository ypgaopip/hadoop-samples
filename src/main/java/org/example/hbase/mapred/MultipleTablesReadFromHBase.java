package org.example.hbase.mapred;

import org.apache.hadoop.conf.Configuration;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 使用MapReduce从多个表读取数据
 */
public class MultipleTablesReadFromHBase {
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
            for (Map.Entry<byte[], byte[]> entry : value.getFamilyMap("info".getBytes()).entrySet()) {
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
        connection = ConnectionFactory.createConnection(conf);

        Job job = Job.getInstance(conf, "MultipleTablesReadFromHBase");
        job.setJarByClass(MultipleTablesReadFromHBase.class);

        List<Scan> scans = new ArrayList<Scan>();
        Scan scan1 = new Scan();
        scan1.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, "chinese".getBytes());
        scans.add(scan1);
        Scan scan2 = new Scan();
        scan2.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, "english".getBytes());
        scans.add(scan2);
        Scan scan3 = new Scan();
        scan3.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, "math".getBytes());
        scans.add(scan3);

        //使用scan的列表作为第一个参数
        TableMapReduceUtil.initTableMapperJob(scans, ReadHBaseMapper.class, Text.class, Text.class, job);
        job.setReducerClass(ReadHBaseReducer.class);

        FileOutputFormat.setOutputPath(job, new Path(args[0]));
        job.waitForCompletion(true);


    }

    /*
    result：参照参数指定hdfs文件的路径：hdfs://10.211.55.4:9000/pip/hbase-reduce/05/
     */

}
