package org.example.hbase.basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class NamespaceCreateTable {

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
        Admin admin = connection.getAdmin();
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create("ns3").build();
        admin.createNamespace(namespaceDescriptor);

        TableName tableName = TableName.valueOf("ns3", "stu");
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(Bytes.toBytes("info"));
        hTableDescriptor.addFamily(hColumnDescriptor);
        admin.createTable(hTableDescriptor);

        boolean tableAvailable = admin.isTableAvailable(tableName);
        System.out.println("table availiable " + tableAvailable);

        if (admin != null) {
            admin.close();
        }
        if (connection != null) {
            connection.close();
        }

    }

    /**
     result

     table availiable true

     executed in hbase shell:

     list_namespace_tables 'ns3'
     TABLE
     stu
     1 row(s)
     Took 0.0159 seconds
     => ["stu"]

     */
}
