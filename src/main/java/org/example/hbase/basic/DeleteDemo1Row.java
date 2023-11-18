package org.example.hbase.basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

public class DeleteDemo1Row {

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

    public static boolean deleteColumn(String tableName, String rowKey) throws IOException {
        createConnection();
        Table table = connection.getTable(TableName.valueOf(tableName));

        Delete delete = new Delete(rowKey.getBytes());
        table.delete(delete);

        System.out.println("删除成功");
        return true;
    }

    public static void main(String[] args) throws IOException {

        deleteColumn("stu", "rw001");

    }

    // before
//    get 'stu', 'rw001'
//    COLUMN                         CELL
//    grade:c                       timestamp=2023-11-16T23:42:37.904, value=80

    //after
//    get 'stu', 'rw001'
//    COLUMN                         CELL
//0 row(s)

}
