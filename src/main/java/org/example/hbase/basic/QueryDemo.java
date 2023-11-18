package org.example.hbase.basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

public class QueryDemo {

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

    public static Result queryByRow(String tableName, String rowKey) throws IOException {
        createConnection();
        Get get = new Get(rowKey.getBytes());
        Table table = connection.getTable(TableName.valueOf(tableName));
        return table.get(get);
    }

    public static void main(String[] args) throws IOException {

        System.out.println("----旧方法----");
        //按行查询数据(旧方法)
        Result result = queryByRow("stu", "rw001");
        List<Cell> cells = result.listCells();
        System.out.println("COLUMN\t\t\tCELL ");
        for (Cell cell : cells) {
            System.out.print(Bytes.toStringBinary(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()) + ":");
            System.out.print(Bytes.toStringBinary(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()) + "\t\t");
            System.out.print("value = " + Bytes.toStringBinary(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()) + ",");
            System.out.println("timestamp = " + cell.getTimestamp());
        }
        System.out.println("----新方法----");

        //按行查询数据(新方法)
        Result result2 = queryByRow("stu", "rw001");
        Cell[] cells2 = result2.rawCells();
        System.out.println("COLUMN\t\t\tCELL ");
        for (Cell cell : cells2) {
            System.out.print(new String(CellUtil.cloneFamily(cell)) + ":");
            System.out.print(new String(CellUtil.cloneQualifier(cell)) + "\t\t");
            System.out.print("value = " + new String(CellUtil.cloneValue(cell)) + ",");
            System.out.println("timestamp = " + cell.getTimestamp());
        }

    }

//    ----旧方法----
//    COLUMN			CELL
//    grade:c		value = 80,timestamp = 1700178157904
//    info:name		value = zhangsan,timestamp = 1700178157430
//            ----新方法----
//    COLUMN			CELL
//    grade:c		value = 80,timestamp = 1700178157904:
//    info:name		value = zhangsan,timestamp = 1700178157430:
}
