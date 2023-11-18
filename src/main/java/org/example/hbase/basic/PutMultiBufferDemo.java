package org.example.hbase.basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.ArrayList;

public class PutMultiBufferDemo {

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

    public static void putBufferList(String tableName, String[] rowKeys, String[] families, String[] columns, String[] values) throws IOException {

        createConnection();
        Admin admin = connection.getAdmin();
        TableName tableNameValue = TableName.valueOf(tableName);
        int length = rowKeys.length;
        ArrayList<Put> putList = new ArrayList<>();
        if (!admin.tableExists(tableNameValue)) {
            System.out.println(tableName + "不存在");
            System.exit(1);
        }
        admin.close();

        // 示例代码用到了，HTable htable = new HTable ...
        // htable.setAutoFlush(false)
        // htable.setWriteBufferSize(64*1024*1024);
        // htable.flushCommits()
        // 等等，这些都已经被废弃了，所以不试验
        // 哎，2018's book，2023 learn，示例代码 hbase-1.3.1, hadoop 2.6.5, zookeeper-3.4.5 真实环境是 hadoop-3.3.5  hbase-2.5.6 zookeeper-3.8.3
        // 与时俱进是积极的，时间力量是无敌的，版本演化是自然的，必有一部分努力是空耗的
        // 自然莫据形，一切随他流，只要心闲时，功到成自然
        // 因为被废弃了，所以和PutMultiDemo.java的代码基本上是一样的
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

    //    get 'stu', 'rw004'
//    COLUMN                         CELL
//    grade:grade                   timestamp=2023-11-17T00:38:10.774, value=86
//    info:name                     timestamp=2023-11-17T00:38:10.774, value=wangwu
    public static void main(String[] args) throws IOException {
        putBufferList("stu", new String[]{"rw004", "rw004"},
                new String[]{"info", "grade"},
                new String[]{"name", "grade"},
                new String[]{"wangwu", "86"});
    }
}
