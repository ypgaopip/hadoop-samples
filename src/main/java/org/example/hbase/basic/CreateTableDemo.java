package org.example.hbase.basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class CreateTableDemo {

    static Configuration conf = null;

    static {
        System.setProperty("HADOOP_USER_NAME", "parallels");
        conf = HBaseConfiguration.create();
        conf.set("hbase.rootdir", "hdfs://10.211.55.4:9000/hbase");
        conf.set("hbase.master", "hdfs://10.211.55.4:16010");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        // 必须使用名称，不能够用ip
        conf.set("hbase.zookeeper.quorum", "ip-10-211-55-4,ip-10-211-55-5,ip-10-211-55-6");
    }

    public static int createTable(String tableName, String[] family) throws IOException {

        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));
        TableName tableNameValue = TableName.valueOf(tableName);

        for (String s : family) {
            HColumnDescriptor column = new HColumnDescriptor(s);
            column.setMaxVersions(3); //指定版本数
            table.addFamily(column); //添加列族
        }
        if (admin.tableExists(tableNameValue)) {
            System.out.println(tableName + "已经存在");
            return -1;
        }
        admin.createTable(table);
        admin.close();
        System.out.println("创建成功");

        return 1;
    }

    public static void main(String[] args) throws IOException {
        createTable("stu", new String[]{"info", "grade"});
    }
}
