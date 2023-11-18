package org.example.hbase.basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.SnapshotDescription;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.List;

public class SnapshotDemo {

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
        TableName tableName = TableName.valueOf("stu2");
        System.out.println("Create snapshot");
        admin.snapshot("snapStu2_1", tableName);
        SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-DD-dd hh:mm:ss");
        List<SnapshotDescription> snapshotDescriptions = admin.listSnapshots();
        System.out.println("List snapshot");
        for (SnapshotDescription snapshotDescription : snapshotDescriptions) {
            System.out.print(snapshotDescription.getName() + "\t" + snapshotDescription.getTableNameAsString() + "\t" + sdf.format(snapshotDescription.getCreationTime()) + "\n");
        }
        System.out.println("Delete snapshot");
        admin.deleteSnapshot("snapStu2_1");
        admin.close();
        if (connection != null) {
            connection.close();
        }

    }

    /**
     result

     Create snapshot
     List snapshot
     snapStu2_1	stu2	2023-11-322-18 08:20:44
     snapStu6	stu6	2023-11-321-17 09:29:25
     Delete snapshot


     */

}
