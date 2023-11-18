package org.example.hbase.basic.filter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ColumnCountGetFilter;

import java.io.IOException;

public class FilterDemo9ColumnCount {

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
        Table table = connection.getTable(TableName.valueOf("stu2"));

        Scan scan = new Scan();
        //如果突然发现一行中的列数超过设定的最大值，则整个扫描操作会停止
        ColumnCountGetFilter filter = new ColumnCountGetFilter(1);
        scan.setFilter(filter);
        ResultScanner scanResult = table.getScanner(scan);

        System.out.println("KEY\t\t\tCOLUMN\t\t\tCELL ");

        for (Result result : scanResult) {
            Cell[] cells = result.rawCells();

            for (Cell cell : cells) {
                System.out.print(new String(CellUtil.cloneRow(cell)) + "\t\t");
                System.out.print(new String(CellUtil.cloneFamily(cell)) + ":");
                System.out.print(new String(CellUtil.cloneQualifier(cell)) + "\t\t");
                System.out.print("value = " + new String(CellUtil.cloneValue(cell)) + ",");
                System.out.println("timestamp = " + cell.getTimestamp());
            }
            System.out.println("-----------------------------------------------------");
        }

    }
    /**
     result

     while count is 2


     KEY			COLUMN			CELL
     rw001		info:age		value = 18,timestamp = 1700188866946
     rw001		info:name		value = lily,timestamp = 1700188862873
     -----------------------------------------------------
     rw002		info:age		value = 25,timestamp = 1700192766047
     rw002		info:name		value = lucy,timestamp = 1700192765974
     -----------------------------------------------------
     rw003		info:age		value = 36,timestamp = 1700192766763
     rw003		info:name		value = zhangsan,timestamp = 1700192766083
     -----------------------------------------------------

     while count is 1

     KEY			COLUMN			CELL
     rw001		info:age		value = 18,timestamp = 1700188866946
     -----------------------------------------------------
     */
}
