package org.example.hbase.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Map读取Mysql，Reduce到HBase
 */
public class MysqlReadFromMysqlWriteToHBaseDemo {

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

    // key : ImmutableBytesWritable, value: Cell, 也就是说value的type只要和reduce的一致就可以
    public static class ReadFromMysqlMapper extends Mapper<LongWritable, StudentBeanNew, LongWritable, Text> {
        // key为id， value为bean的to string返回，为空格分隔
        @Override
        protected void map(LongWritable key, StudentBeanNew value, Mapper<LongWritable, StudentBeanNew, LongWritable, Text>.Context context) throws IOException, InterruptedException {
            context.write(new LongWritable(value.id), new Text(value.toString())); //这个toString内容的分隔符是空格
        }
    }

    public static class WriteToHBaseReduce extends TableReducer<LongWritable, Text, ImmutableBytesWritable> {
        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Reducer<LongWritable, Text, ImmutableBytesWritable, Mutation>.Context context) throws IOException, InterruptedException {

            //构建写入db用的bean
            StudentBeanNew studentBean = new StudentBeanNew();
            //loop row key对应的行，得到name和age，并设定到student bean里面

            for (Text value : values) {
                String[] line = value.toString().split(" "); //这个地方使用空格，和toString内容的分隔符是空格是对应的
                //实际上，这个地方不需要使用studentBean的了，直接用lin的分隔后的值就可以了
                studentBean.id = Integer.parseInt(line[0]);
                studentBean.name = line[1];
                studentBean.sex = line[2];
                studentBean.age = Integer.parseInt(line[3]);
                //写入db
                //value应该用null也是可以的
                Put put = new Put(studentBean.name.getBytes());
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id"), Bytes.toBytes(String.valueOf(studentBean.id)));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("sex"), Bytes.toBytes(studentBean.sex));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(String.valueOf(studentBean.age)));
                context.write(new ImmutableBytesWritable(studentBean.name.getBytes()), put);
            }
        }
    }


    private static void checkTable(Configuration conf) throws IOException {
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();
        TableName tn = TableName.valueOf("stu2HBase");
        if (!admin.tableExists(tn)) {
            ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.of("info");
            TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tn).setColumnFamily(columnFamilyDescriptor).build();
            admin.createTable(tableDescriptor);
            System.out.println("表不存在，创建" + tn.getNameAsString() + "成功");
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        checkTable(conf);

        String srcMysqlTableName = "stu";
        // 因为reduce到hbase中，所以这段要被替换掉
//        String destMysqlTableName = "stu2";
        String destHBaseTableName = "stu2HBase";
        // 因为reduce到hbase中，所以这段要被替换掉 end

        DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver",
                "jdbc:mysql://10.211.55.4:3306/hbase", "root", "123");

        Job job = Job.getInstance(conf, "read from mysql table write to HBase");
        job.setJarByClass(MysqlReadFromMysqlWriteToHBaseDemo.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(ReadFromMysqlMapper.class);

        // 因为reduce到hbase中，所以这段要被替换掉
//        job.setReducerClass(WriteToMysqlReduce.class);
//        job.setOutputKeyClass(LongWritable.class);
//        job.setOutputValueClass(Text.class);

//        job.setOutputFormatClass(DBOutputFormat.class);
        // 因为reduce到hbase中，所以这段要被替换掉 end

        job.setInputFormatClass(DBInputFormat.class);

        // 因为reduce到hbase中，所以被替换掉，用这段替换
        TableMapReduceUtil.initTableReducerJob(destHBaseTableName, WriteToHBaseReduce.class, job, null, null, null, null, false);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Put.class);
        // 因为reduce到hbase中，所以被替换掉，用这段替换 end

        String[] fields = {"id", "name", "sex", "age"};

        //job, db.class, table name, conditions, order by and fields
        DBInputFormat.setInput(job, StudentBeanNew.class, srcMysqlTableName, null, null, fields);

        //add db jar
        String jarPath = "hdfs://10.211.55.4:9000/pip/libs/db/mysql-connector-java-8.0.19.jar";
        job.addArchiveToClassPath(new Path(jarPath));
        DistributedCache.addFileToClassPath(new Path(jarPath), conf);

        // 因为reduce到hbase中，所以这段不需要
        //DB输出
//        DBOutputFormat.setOutput(job, destMysqlTableName, fields);
        // 因为reduce到hbase中，所以这段不需要 end
        job.waitForCompletion(true);

    }

    /*

    before:
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

    after:
         scan 'stu2HBase'
        ROW                                         COLUMN+CELL
         john                                       column=info:age, timestamp=2023-11-19T09:57:57.378, value=18
         john                                       column=info:id, timestamp=2023-11-19T09:57:57.378, value=2
         john                                       column=info:sex, timestamp=2023-11-19T09:57:57.378, value=male
         linda                                      column=info:age, timestamp=2023-11-19T09:57:57.378, value=17
         linda                                      column=info:id, timestamp=2023-11-19T09:57:57.378, value=3
         linda                                      column=info:sex, timestamp=2023-11-19T09:57:57.378, value=female
         lucy                                       column=info:age, timestamp=2023-11-19T09:57:57.378, value=16
         lucy                                       column=info:id, timestamp=2023-11-19T09:57:57.378, value=1
         lucy                                       column=info:sex, timestamp=2023-11-19T09:57:57.378, value=female
         ming                                       column=info:age, timestamp=2023-11-19T09:57:57.378, value=11
         ming                                       column=info:id, timestamp=2023-11-19T09:57:57.378, value=5
         ming                                       column=info:sex, timestamp=2023-11-19T09:57:57.378, value=male
         niki                                       column=info:age, timestamp=2023-11-19T09:57:57.378, value=13
         niki                                       column=info:id, timestamp=2023-11-19T09:57:57.378, value=6
         niki                                       column=info:sex, timestamp=2023-11-19T09:57:57.378, value=male
         smith                                      column=info:age, timestamp=2023-11-19T09:57:57.378, value=20
         smith                                      column=info:id, timestamp=2023-11-19T09:57:57.378, value=4
         smith                                      column=info:sex, timestamp=2023-11-19T09:57:57.378, value=male

     */

}
