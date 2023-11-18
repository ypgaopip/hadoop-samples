package org.example.hbase.basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.ArrayList;

public class PutMultiDemo {

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

    public static void putList(String tableName, String[] rowKeys, String[] families, String[] columns, String[] values) throws IOException {

        createConnection();
        Admin admin = connection.getAdmin();
        TableName tableNameValue = TableName.valueOf(tableName);
        int length = rowKeys.length;
        ArrayList<Put> putList = new ArrayList<>();
        if (!admin.tableExists(tableNameValue)) {
            System.out.println(tableName + "不存在");
            System.exit(1);
        }
        for (int i = 0; i < length; i++) {
            Put put = new Put(rowKeys[i].getBytes());
            Table table = connection.getTable(TableName.valueOf(tableName));
            put.addColumn(families[i].getBytes(), columns[i].getBytes(), values[i].getBytes());
            putList.add(put);
            table.put(putList);
            table.close();
            System.out.println("插入成功");
        }

    }

    // get 'stu', 'rw002'
//    COLUMN                         CELL
//    grade:Java                    timestamp=2023-11-16T23:54:40.777, value=70
//    info:name                     timestamp=2023-11-16T23:54:40.777, value=60
    public static void main(String[] args) throws IOException {
        putList("stu", new String[]{"rw002", "rw002"},
                new String[]{"info", "grade"},
                new String[]{"name", "Java"},
                new String[]{"60", "70"});
    }
}
