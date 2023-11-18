package org.example.hbase.basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class PutCheckAndPutDemo {

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

    public static void checkAndPut(String tableName, String rowkey, String family, String column, String value) throws IOException {

        createConnection();
        Admin admin = connection.getAdmin();
        TableName tableNameValue = TableName.valueOf(tableName);

        if (!admin.tableExists(tableNameValue)) {
            System.out.println(tableName + "不存在");
            System.exit(1);
        }

        Put put = new Put(rowkey.getBytes());
        Table table = connection.getTable(TableName.valueOf(tableName));
        put.addColumn(family.getBytes(), column.getBytes(), value.getBytes());
        table.checkAndPut(Bytes.toBytes(rowkey), Bytes.toBytes(family), Bytes.toBytes(column), null, put);
        System.out.println("插入成功");

    }

    // 方法已经被deprecated了
//    get 'stu', 'rw003'
//    COLUMN                         CELL
//    grade:C++                     timestamp=2023-11-17T00:11:13.082, value=75
//    info:name                     timestamp=2023-11-17T00:11:12.974, value=lisi
    // checkAndPut 提供原子性操作，如果失败，所以更改都失效，在多个客户端对同一个数据进行修改将会提供较高的效率
    public static void main(String[] args) throws IOException {
        checkAndPut("stu", "rw003", "info", "name", "lisi");
        checkAndPut("stu", "rw003", "grade", "C++", "75");
    }
}
