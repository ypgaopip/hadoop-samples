package org.example.hbase.basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class IncrementSingleColumn {

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


    public static void main(String[] args) throws IOException {
        createConnection();
        Table table = connection.getTable(TableName.valueOf("counters2"));

        // 增加（行号，列族，列，步长）
        long cnt1 = table.incrementColumnValue(Bytes.toBytes("20180131"), Bytes.toBytes("daily"), Bytes.toBytes("counter"), 1);
        System.out.println(cnt1);
        long cnt2 = table.incrementColumnValue(Bytes.toBytes("20180131"), Bytes.toBytes("daily"), Bytes.toBytes("counter"), 0);
        System.out.println(cnt2);
        long cnt3 = table.incrementColumnValue(Bytes.toBytes("20180131"), Bytes.toBytes("daily"), Bytes.toBytes("counter"), -1);
        System.out.println(cnt3);
        long cnt4 = table.incrementColumnValue(Bytes.toBytes("20180131"), Bytes.toBytes("daily"), Bytes.toBytes("counter"), 20);
        System.out.println(cnt4);

        table.close();
        if (connection != null) {
            connection.close();
        }
    }

    /**
     prepare
     create 'counters2', 'daily','weekly','monthly'

     result

     1
     1
     0
     20

     */
}
