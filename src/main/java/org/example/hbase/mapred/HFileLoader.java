package org.example.hbase.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;


/**
 * HFile入库到HBase
 */
public class HFileLoader {
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

    public static void main(String[] args) throws Exception {
        connection = ConnectionFactory.createConnection(conf);
        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);

        TableName stu71 = TableName.valueOf("stu7");
        Admin admin = connection.getAdmin();
        Table table = admin.getConnection().getTable(stu71);
        RegionLocator stu7RegionLocator = connection.getRegionLocator(stu71);

        loader.doBulkLoad(new Path(args[1]), admin,
                table, stu7RegionLocator);
    }

}

/*

    result：

    scan 'stu7'
    ROW                                         COLUMN+CELL
     rk001                                      column=info:age, timestamp=2023-11-19T06:31:39.671, value=16
     rk001                                      column=info:name, timestamp=2023-11-19T06:31:39.671, value=lucy
     rk002                                      column=info:age, timestamp=2023-11-19T06:31:39.671, value=18
     rk002                                      column=info:name, timestamp=2023-11-19T06:31:39.671, value=lily
     rk003                                      column=info:age, timestamp=2023-11-19T06:31:39.671, value=12
     rk003                                      column=info:name, timestamp=2023-11-19T06:31:39.672, value=xiaoming
    3 row(s)
    Took 0.1059 seconds

hdfs查看：
    之前的hbase文件不存在了
    hdfs dfs -ls -R /pip/hbase-reduce/03
    -rw-r--r--   3 parallels supergroup          0 2023-11-19 06:31 /pip/hbase-reduce/03/_SUCCESS
    drwxr-xr-x   - parallels supergroup          0 2023-11-19 06:44 /pip/hbase-reduce/03/info
 */
