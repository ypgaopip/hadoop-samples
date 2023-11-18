package org.example.hbase.basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

public class ScanDemo1 {

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

    public static ResultScanner scan(String tableName, String rowKey) throws IOException {
        createConnection();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();

        return table.getScanner(scan);
    }

    //    put 'stu2', 'rw002','info:name','lucy'
//    put 'stu2', 'rw002','info:age','20'
//    put 'stu2', 'rw002','info:age','25'
//    put 'stu2', 'rw003','info:name','zhangsan'
//    put 'stu2', 'rw003','info:age','32'
//    put 'stu2', 'rw003','info:age','36'
    public static void main(String[] args) throws IOException {


        ResultScanner scan = scan("stu2", "rw001");
        System.out.println("COLUMN\t\t\tCELL ");

        for (Result result : scan) {
            Cell[] cells = result.rawCells();

            for (Cell cell : cells) {
                System.out.print(new String(CellUtil.cloneFamily(cell)) + ":");
                System.out.print(new String(CellUtil.cloneQualifier(cell)) + "\t\t");
                System.out.print("value = " + new String(CellUtil.cloneValue(cell)) + ",");
                System.out.println("timestamp = " + cell.getTimestamp());
            }
        }

    }

}
