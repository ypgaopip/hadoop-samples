package org.example.hbase.basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class CoprocessorDemo {

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

    public static void addTableCoprocessor(String tname, String coprocessorClassName) throws IOException {
        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf(tname);
        HTableDescriptor tableDescriptorOld = admin.getTableDescriptor(tableName);
        HTableDescriptor tableDescriptor = new HTableDescriptor(tableDescriptorOld);
        boolean readOnly = tableDescriptor.isReadOnly();
        System.out.println("table " + tableName + " readonly: " + readOnly);
        boolean tableEnabled = admin.isTableEnabled(tableName);
        System.out.println("table " + tableName + " status: " + tableEnabled);
//        admin.enableTable(tableName);
//        System.exit(0);
        if (tableEnabled) {
            admin.disableTable(tableName);
        }
        tableDescriptor.addCoprocessor(coprocessorClassName);
//        tableDescriptor.removeCoprocessor(coprocessorClassName);
        admin.modifyTable(tableName, tableDescriptor);
        admin.enableTable(tableName);

    }

    public static long rowCount(String tableName, String family) throws Throwable {
        long rowCount = 0;
        AggregationClient aggregationClient = new AggregationClient(conf);
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(family));
        rowCount = aggregationClient.rowCount(TableName.valueOf(tableName), new LongColumnInterpreter(), scan);
        return rowCount;
    }


    public static void main(String[] args) throws Throwable {

        createConnection();
        String coprocessorClassName = "org.apache.hadoop.hbase.coprocessor.AggregateImplementation";
        CoprocessorDemo.addTableCoprocessor("stu6", coprocessorClassName);
        long rowCount = CoprocessorDemo.rowCount("stu6", "info");
        System.out.println("rowCount: " + rowCount);
    }

    /**

     注意点：
     1）如果是disable状态，不能两次disableTable
     可用如下来enable
     //        admin.enableTable(tableName);
     //        System.exit(0);
     2）出现HTableDescriptor is read-only问题，新建一个对象解决
     HTableDescriptor tableDescriptorOld = admin.getTableDescriptor(tableName);
     HTableDescriptor tableDescriptor = new HTableDescriptor(tableDescriptorOld);
     3）不能重复调用 tableDescriptor.addCoprocessor(coprocessorClassName);
     可以使用 tableDescriptor.removeCoprocessor(coprocessorClassName);来remove

     prepare

     pom.xml needs to import the follows
     <dependency>
     <groupId>org.apache.hbase</groupId>
     <artifactId>hbase-endpoint</artifactId>
     <version>2.5.6-hadoop3</version>
     </dependency>

     result

     table stu6 readonly: false
     table stu6 status: true
     rowCount: 3


     看一下，已经加入了coprocessor$1
     desc 'stu6'
     Table stu6 is ENABLED
     stu6, {TABLE_ATTRIBUTES => {coprocessor$1 => '|org.apache.hadoop.hbase.coprocessor.AggregateImplementation|1073741823|', METADATA => {'hbase.store.file-tracker.impl' => 'D
     EFAULT'}}}
     COLUMN FAMILIES DESCRIPTION
     {NAME => 'grade', INDEX_BLOCK_ENCODING => 'NONE', VERSIONS => '1', KEEP_DELETED_CELLS => 'FALSE', DATA_BLOCK_ENCODING => 'NONE', TTL => 'FOREVER', MIN_VERSIONS => '0', REP
     LICATION_SCOPE => '0', BLOOMFILTER => 'ROW', IN_MEMORY => 'false', COMPRESSION => 'NONE', BLOCKCACHE => 'true', BLOCKSIZE => '65536 B (64KB)'}

     {NAME => 'info', INDEX_BLOCK_ENCODING => 'NONE', VERSIONS => '1', KEEP_DELETED_CELLS => 'FALSE', DATA_BLOCK_ENCODING => 'NONE', TTL => 'FOREVER', MIN_VERSIONS => '0', REPL
     ICATION_SCOPE => '0', BLOOMFILTER => 'ROW', IN_MEMORY => 'false', COMPRESSION => 'NONE', BLOCKCACHE => 'true', BLOCKSIZE => '65536 B (64KB)'}

     2 row(s)
     Quota is disabled
     Took 0.0639 seconds

     */
}
