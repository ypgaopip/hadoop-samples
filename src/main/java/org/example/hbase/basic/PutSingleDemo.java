package org.example.hbase.basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

public class PutSingleDemo {

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

    public static void insert(String tableName, String rowkey, String family, String column, String cell) throws IOException {

        createConnection();
        Put put = new Put(rowkey.getBytes());
        Table table = connection.getTable(TableName.valueOf(tableName));
        put.addColumn(family.getBytes(), column.getBytes(), cell.getBytes());
        table.put(put);
        System.out.println("插入成功");

    }

    public static void main(String[] args) throws IOException {
        insert("stu", "rw001", "info", "name", "zhangsan");
        insert("stu", "rw001", "grade", "c", "80");
    }
}
