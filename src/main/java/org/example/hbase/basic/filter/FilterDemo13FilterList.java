package org.example.hbase.basic.filter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;

public class FilterDemo13FilterList {

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

    //等于lucy，且不为rw001
    public static void main(String[] args) throws IOException {
        createConnection();
        Table table = connection.getTable(TableName.valueOf("stu2"));

        Scan scan = new Scan();
        ValueFilter vf = new ValueFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("lucy"));
        RowFilter rf = new RowFilter(CompareFilter.CompareOp.NOT_EQUAL, new BinaryComparator(Bytes.toBytes("rw001")));
        ArrayList<Filter> filters = new ArrayList<>();
        filters.add(rf);
        filters.add(vf);
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, filters);
        scan.setFilter(filterList);
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
     rw002		info:name		value = lucy,timestamp = 1700192765974
     -----------------------------------------------------
     */
}
