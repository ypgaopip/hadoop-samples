package org.example.hbase.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 用MapReduce读取表数据，写入到另外一个表里面，相当于导入导出
 */
public class MysqlReadFromMysqlWriteToSQLDemo {

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

    public static class StudentBeanNew implements Writable, DBWritable {

        private Integer id;
        private String name;
        private String sex;
        private Integer age;


        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeInt(this.id);
            dataOutput.writeUTF(this.name);
            dataOutput.writeUTF(this.sex);
            dataOutput.writeInt(this.age);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.id = dataInput.readInt();
            this.name = Text.readString(dataInput);
            this.sex = Text.readString(dataInput);
            this.age = dataInput.readInt();
        }

        @Override
        public void write(PreparedStatement preparedStatement) throws SQLException {
            preparedStatement.setInt(1, this.id);
            preparedStatement.setString(2, this.name);
            preparedStatement.setString(3, this.sex);
            preparedStatement.setInt(4, this.age);
        }

        @Override
        public void readFields(ResultSet resultSet) throws SQLException {
            this.id = resultSet.getInt(1);
            this.name = resultSet.getString(2);
            this.sex = resultSet.getString(3);
            this.age = resultSet.getInt(4);
        }

        public Integer getId() {
            return id;
        }

        public void setId(Integer id) {
            this.id = id;
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
            return new String(id + " " + name + " " + sex + " " + age);
        }
    }

    // key : ImmutableBytesWritable, value: Text
    public static class ReadFromMysqlMapper extends Mapper<LongWritable, StudentBeanNew, LongWritable, Text> {
        // key为id， value为bean的to string返回，为空格分隔
        @Override
        protected void map(LongWritable key, StudentBeanNew value, Mapper<LongWritable, StudentBeanNew, LongWritable, Text>.Context context) throws IOException, InterruptedException {
            context.write(new LongWritable(value.id), new Text(value.toString()));
        }
    }

    public static class WriteToMysqlReduce extends Reducer<LongWritable, Text, StudentBeanNew, Text> {
        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Reducer<LongWritable, Text, StudentBeanNew, Text>.Context context) throws IOException, InterruptedException {

            //构建写入db用的bean
            StudentBeanNew studentBean = new StudentBeanNew();
            //loop row key对应的行，得到name和age，并设定到student bean里面
            for (Text value : values) {
                String[] line = value.toString().split(" ");
                studentBean.id = Integer.parseInt(line[0]);
                studentBean.name = line[1];
                studentBean.sex = line[2];
                studentBean.age = Integer.parseInt(line[3]);
                //写入db
                //value应该用null也是可以的
                context.write(studentBean, null);
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        String srcMysqlTableName = "stu";
        String destMysqlTableName = "stu2";

        DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver",
                "jdbc:mysql://10.211.55.4:3306/hbase", "root", "123");

        Job job = Job.getInstance(conf, "read from mysql table write to mysql");
        job.setJarByClass(MysqlReadFromMysqlWriteToSQLDemo.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(ReadFromMysqlMapper.class);

        job.setReducerClass(WriteToMysqlReduce.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(DBOutputFormat.class);
        job.setInputFormatClass(DBInputFormat.class);

        String[] fields = {"id", "name", "sex", "age"};

        //job, db.class, table name, conditions, order by and fields
        DBInputFormat.setInput(job, StudentBeanNew.class, srcMysqlTableName, null, null, fields);

        //add db jar
        String jarPath = "hdfs://10.211.55.4:9000/pip/libs/db/mysql-connector-java-8.0.19.jar";
        job.addArchiveToClassPath(new Path(jarPath));
        DistributedCache.addFileToClassPath(new Path(jarPath), conf);

        //DB输出
        DBOutputFormat.setOutput(job, destMysqlTableName, fields);
        job.waitForCompletion(true);

    }

    /*
    prepare

          create table stu2(id int, name varchar(100), sex varchar(100), age int);
          create table stu(id int, name varchar(100), sex varchar(100), age int);
            insert into stu values(1,"lucy","female",16);
            insert into stu values(2,"john","male",18);
            insert into stu values(3,"linda","female",17);
            insert into stu values(4,"smith","male",20);
            insert into stu values(5,"ming","male",11);
            insert into stu values(6,"niki","male",13);

            select * from stu;
              +------+-------+--------+------+
            | id   | name  | sex    | age  |
            +------+-------+--------+------+
            |    1 | lucy  | female |   16 |
            |    2 | john  | male   |   18 |
            |    3 | linda | female |   17 |
            |    4 | smith | male   |   20 |
            |    5 | ming  | male   |   11 |
            |    6 | niki  | male   |   13 |
            +------+-------+--------+------+
            6 rows in set (0.00 sec)

            select * from stu2;
                    Empty set (0.01 sec)


    after run:
    查询，有数据，说明已经成功，导出导入
        select * from stu2;
+------+-------+--------+------+
| id   | name  | sex    | age  |
+------+-------+--------+------+
|    1 | lucy  | female |   16 |
|    2 | john  | male   |   18 |
|    3 | linda | female |   17 |
|    4 | smith | male   |   20 |
|    5 | ming  | male   |   11 |
|    6 | niki  | male   |   13 |
+------+-------+--------+------+

     */

}
