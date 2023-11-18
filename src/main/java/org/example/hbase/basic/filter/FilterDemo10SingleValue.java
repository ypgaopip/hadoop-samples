package org.example.hbase.basic.filter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class FilterDemo10SingleValue {

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
        //过滤出name列的值包含li的信息
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("name"),
                CompareOperator.EQUAL, new SubstringComparator("li"));
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

     KEY			COLUMN			CELL
     rw001		info:age		value = 18,timestamp = 1700188866946
     rw001		info:name		value = lily,timestamp = 1700188862873
     -----------------------------------------------------
     */
}
