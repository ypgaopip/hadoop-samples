package org.example.hbase.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

/**
 * 使用MapReduce复制HBase表
 */
public class HBaseTableCopyDemo {

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

    // key : ImmutableBytesWritable, value: Put
    public static class ReadHBaseMapper extends TableMapper<ImmutableBytesWritable, Put> {
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Put>.Context context) throws IOException, InterruptedException {
            //get cell, equals hbase's table's search result
            List<Cell> cells = value.listCells();
            //build put with row key
            Put put = new Put(key.get());
            //load into put
            for (Cell cell : cells) {
                put.add(cell);
            }
            //写入reduce
            context.write(key, put);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String srcName = "stu6";
        String destName = "stu8";

        Job job = Job.getInstance(conf, "HBase table copy");
        job.setJarByClass(HBaseTableCopyDemo.class);
        job.setNumReduceTasks(0);

        Scan scan = new Scan();
        scan.setCaching(10000); //1是Scan中的默认值，这将对MapReduce作业不利
        scan.setCacheBlocks(false); //不要将MR作业设置成true

        TableMapReduceUtil.initTableMapperJob(srcName, scan, ReadHBaseMapper.class, ImmutableBytesWritable.class, Result.class, job);
        TableMapReduceUtil.initTableReducerJob(destName, null, job);

        job.waitForCompletion(true);


    }

    /*


    before
    create 'stu8','info'

    scan 'stu6'
ROW                                         COLUMN+CELL
 rw001                                      column=info:age, timestamp=2023-11-17T11:22:48.636, value=16
 rw001                                      column=info:name, timestamp=2023-11-17T11:22:48.613, value=Lucy
 rw002                                      column=info:age, timestamp=2023-11-17T11:22:48.675, value=18
 rw002                                      column=info:name, timestamp=2023-11-17T11:22:48.656, value=Linda
 rw003                                      column=info:age, timestamp=2023-11-17T11:22:49.226, value=19
 rw003                                      column=info:name, timestamp=2023-11-17T11:22:48.687, value=John

scan 'stu8'
ROW                                         COLUMN+CELL
0 row(s)


    after:

     scan 'stu8'
ROW                                         COLUMN+CELL
 rw001                                      column=info:age, timestamp=2023-11-17T11:22:48.636, value=16
 rw001                                      column=info:name, timestamp=2023-11-17T11:22:48.613, value=Lucy
 rw002                                      column=info:age, timestamp=2023-11-17T11:22:48.675, value=18
 rw002                                      column=info:name, timestamp=2023-11-17T11:22:48.656, value=Linda
 rw003                                      column=info:age, timestamp=2023-11-17T11:22:49.226, value=19
 rw003                                      column=info:name, timestamp=2023-11-17T11:22:48.687, value=John
3 row(s)


     */

}
