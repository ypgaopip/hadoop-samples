package org.example.hbase.basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;

import java.io.IOException;

public class CountTable {

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

    private static void createConnection() throws IOException {
        connection = ConnectionFactory.createConnection(conf);
    }

    public static long rowCount(String tableName) throws IOException {
        createConnection();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        scan.setFilter(new FirstKeyOnlyFilter());
        ResultScanner scanner = table.getScanner(scan);
        long count = 0;
        for (Result result : scanner) {
            count += result.size();
        }
        return count;
    }

    public static void main(String[] args) throws IOException {
        System.out.println(rowCount("stu2"));
    }

    /**
     result

     3

     executed in hbase shell:

     count 'stu2', INTERVAL=>1000, CACHE=>1000
     3 row(s)
     Took 0.0733 seconds
     => 3

     */
}
