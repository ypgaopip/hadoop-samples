package org.example.hbase.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * 读取hbase表内容，map
 * 写入mysql表内容，reduce
 */
public class MysqlReadFromHBaseWriteSQLDemo {

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

    public static class StudentBean implements Writable, DBWritable {

        private String name;
        private String sex;
        private Integer age;


        @Override
        public void write(DataOutput dataOutput) throws IOException {
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {

        }

        @Override
        public void write(PreparedStatement preparedStatement) throws SQLException {
            preparedStatement.setString(1, this.name);
            preparedStatement.setString(2, this.sex);
            preparedStatement.setInt(3, this.age);
        }

        @Override
        public void readFields(ResultSet resultSet) throws SQLException {
            this.name = resultSet.getString(1);
            this.sex = resultSet.getString(2);
            this.age = resultSet.getInt(3);
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getSex() {
            return sex;
        }

        public void setSex(String sex) {
            this.sex = sex;
        }

        public Integer getAge() {
            return age;
        }

        public void setAge(Integer age) {
            this.age = age;
        }

        @Override
        public String toString() {
            return new String(name + " " + sex + " " + age);
        }
    }

    // key : ImmutableBytesWritable, value: Cell, 也就是说value的type只要和reduce的一致就可以，会出现如下错误
//    java.io.IOException: Type mismatch in value from map: expected org.apache.hadoop.hbase.Cell, received org.apache.hadoop.hbase.NoTagsKeyValue
    // 改用value， Text类型，内容为 列族，列，值（逗号分隔）
    public static class ReadHBaseMapper extends TableMapper<ImmutableBytesWritable, Text> {
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Text>.Context context) throws IOException, InterruptedException {
            //get cell, equals hbase's table's search result
            List<Cell> cells = value.listCells();
            for (Cell cell : cells) {
                //写入reduce
                //行键为key，Cell为value
                byte[] cloneFamily = CellUtil.cloneFamily(cell);
                byte[] cloneQualifier = CellUtil.cloneQualifier(cell);
                byte[] cloneValue = CellUtil.cloneValue(cell);
                String s = Bytes.toString(cloneFamily) + "," + Bytes.toString(cloneQualifier) + "," + Bytes.toString(cloneValue) + ",";
                context.write(new ImmutableBytesWritable(CellUtil.cloneRow(cell)), new Text(s));
            }
        }
    }

    public static class WriteToMysqlReduce extends Reducer<ImmutableBytesWritable, Text, StudentBean, Text> {
        @Override
        protected void reduce(ImmutableBytesWritable key, Iterable<Text> values, Reducer<ImmutableBytesWritable, Text, StudentBean, Text>.Context context) throws IOException, InterruptedException {
            //构建db用的bean
            String keyStr = Bytes.toString(key.get());
            StudentBean studentBean = new StudentBean();
            //loop row key对应的行，得到name和age，并设定到student bean里面
            for (Text value : values) {
                String[] split = value.toString().split(",");
                String family = split[0];
                String qualifier = split[1];
                String cellValue = split[2];
                studentBean.setSex(keyStr + "-" + family);
                if (qualifier.equals("name")) {
                    studentBean.setName(cellValue);
                }
                if (qualifier.equals("age")) {
                    studentBean.setAge(Integer.parseInt(cellValue));
                }
            }
            //写入db
            context.write(studentBean, null);
        }

    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {


        String srcHBaseTableName = "stu6";
        String targetMysqlTableName = "stuInfo";

        DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver",
                "jdbc:mysql://10.211.55.4:3306/hbase", "root", "123");

        Job job = Job.getInstance(conf, "read from HBase table write to mysql");
        job.setJarByClass(MysqlReadFromHBaseWriteSQLDemo.class);

        Scan scan = new Scan();

        TableMapReduceUtil.initTableMapperJob(srcHBaseTableName, scan, ReadHBaseMapper.class, ImmutableBytesWritable.class, Text.class, job);
        job.setReducerClass(WriteToMysqlReduce.class);

//        job.setOutputFormatClass(DBOutputFormat.class);

        //add db jar
        String jarPath = "hdfs://10.211.55.4:9000/pip/libs/db/mysql-connector-java-8.0.19.jar";
        job.addArchiveToClassPath(new Path(jarPath));
        DistributedCache.addFileToClassPath(new Path(jarPath), conf);

        //DB输出
        DBOutputFormat.setOutput(job, targetMysqlTableName, "name", "sex", "age");
        job.waitForCompletion(true);

    }

    /*

    prepare:
        create database hbase;
        use hbase;
        create table stuInfo(name varchar(100), sex varchar(100), age int);

    hbase：
        scan 'stu6'
        ROW                                         COLUMN+CELL
         rw001                                      column=info:age, timestamp=2023-11-17T11:22:48.636, value=16
         rw001                                      column=info:name, timestamp=2023-11-17T11:22:48.613, value=Lucy
         rw002                                      column=info:age, timestamp=2023-11-17T11:22:48.675, value=18
         rw002                                      column=info:name, timestamp=2023-11-17T11:22:48.656, value=Linda
         rw003                                      column=info:age, timestamp=2023-11-17T11:22:49.226, value=19
         rw003                                      column=info:name, timestamp=2023-11-17T11:22:48.687, value=John

    run result:
        select * from stuInfo;
        +-------+------------+------+
        | name  | sex        | age  |
        +-------+------------+------+
        | Lucy  | rw001-info |   16 |
        | Linda | rw002-info |   18 |
        | John  | rw003-info |   19 |
        +-------+------------+------+
        3 rows in set (0.01 sec)


     */
}
