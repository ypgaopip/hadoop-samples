package org.example.hbase.basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class IncrementMultiColumns {

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
        Table table = connection.getTable(TableName.valueOf("counters3"));

        // 增加（行号，列族，列，步长）
        Increment increment = new Increment(Bytes.toBytes("20180131"));
        increment.addColumn(Bytes.toBytes("daily"), Bytes.toBytes("counter1"), 1);
        increment.addColumn(Bytes.toBytes("daily"), Bytes.toBytes("counter2"), 1);
        increment.addColumn(Bytes.toBytes("weekly"), Bytes.toBytes("counter1"), 10);
        increment.addColumn(Bytes.toBytes("weekly"), Bytes.toBytes("counter2"), 10);

        Result result = table.increment(increment);
        for (Cell cell : result.rawCells()) {
            System.out.println("Cell:" + cell + " Value:" + Bytes.toLong(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
        }

        Increment increment2 = new Increment(Bytes.toBytes("20180131"));
        increment2.addColumn(Bytes.toBytes("daily"), Bytes.toBytes("counter1"), 5);
        increment2.addColumn(Bytes.toBytes("daily"), Bytes.toBytes("counter2"), 1);
        increment2.addColumn(Bytes.toBytes("weekly"), Bytes.toBytes("counter1"), 0);
        increment2.addColumn(Bytes.toBytes("weekly"), Bytes.toBytes("counter2"), -5);

        Result result2 = table.increment(increment2);
        for (Cell cell : result2.rawCells()) {
            System.out.println("Cell:" + cell + " Value:" + Bytes.toLong(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
        }


        table.close();
        if (connection != null) {
            connection.close();
        }
    }

    /**
     prepare
     create 'counters3', 'daily','weekly','monthly'

     result

     Cell:20180131/daily:counter1/1700219093712/Put/vlen=8/seqid=0 Value:1
     Cell:20180131/daily:counter2/1700219093712/Put/vlen=8/seqid=0 Value:1
     Cell:20180131/weekly:counter1/1700219093712/Put/vlen=8/seqid=0 Value:10
     Cell:20180131/weekly:counter2/1700219093712/Put/vlen=8/seqid=0 Value:10
     Cell:20180131/daily:counter1/1700219093752/Put/vlen=8/seqid=0 Value:6
     Cell:20180131/daily:counter2/1700219093752/Put/vlen=8/seqid=0 Value:2
     Cell:20180131/weekly:counter1/1700219093752/Put/vlen=8/seqid=0 Value:10
     Cell:20180131/weekly:counter2/1700219093752/Put/vlen=8/seqid=0 Value:5

     executed in hbase shell
     hbase:081:0> get_counter 'counters3', '20180131','daily:counter1'
     COUNTER VALUE = 6
     Took 0.0061 seconds

     */
}
