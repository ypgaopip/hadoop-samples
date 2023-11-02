package org.example.mapredext;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.StringTokenizer;


/**
 * 输出到Mysql中，第一列为word，第二列为次数
 */
public class MysqlOutputsSample {

    public static class TblsWritable implements Writable, DBWritable {
        String word;
        int times;

        public TblsWritable() {

        }

        public TblsWritable(String word, int times) {
            this.word = word;
            this.times = times;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(word);
            dataOutput.writeInt(times);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            word = dataInput.readUTF();
            times = dataInput.readInt();

        }

        @Override
        public void write(PreparedStatement preparedStatement) throws SQLException {
            preparedStatement.setString(1, this.word);
            preparedStatement.setInt(2, this.times);
        }

        @Override
        public void readFields(ResultSet resultSet) throws SQLException {
            this.word = resultSet.getString(1);
            this.times = resultSet.getInt(2);
        }

        @Override
        public String toString() {
            return new String(word + " " + times);
        }
    }


    public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        private final static LongWritable one = new LongWritable(1);
        private Text word = new Text();


        //key是word，value是1
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            if (value != null) {
                String line = value.toString();
                StringTokenizer stringTokenizer = new StringTokenizer(line);
                while (stringTokenizer.hasMoreTokens()) {
                    word.set(stringTokenizer.nextToken());
                    context.write(word, one);
                }
            }
        }
    }


    public static class IntSumReducer extends Reducer<Text, LongWritable, TblsWritable, LongWritable> {

        //out key是db，out value是null
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, TblsWritable, LongWritable>.Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();

            }
            context.write(new TblsWritable(key.toString(), (int) sum), null);
        }
    }


    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();

        //在hadoop集群中可以不用
        conf.set("fs.defaultFS", "hdfs://10.211.55.4:9000");
        System.setProperty("HADOOP_USER_NAME", "parallels");
        //在hadoop集群中可以不用 end

        //I fule you!, 这个配置必须在job创建之前啊，这个地方是有顺序的，it spend three hours.
        //set db
        DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver",
                "jdbc:mysql://10.211.55.4:3306/hadoop", "root", "123");

        Job job = Job.getInstance(conf, "Mysql outputs sample");
        job.setJarByClass(MysqlOutputsSample.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(TblsWritable.class); //?
        job.setOutputValueClass(LongWritable.class);

        //delete output path
        Path outputDir = new Path(args[1]);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputDir)) {
            fs.delete(outputDir, true);
        }

        FileInputFormat.addInputPath(job, new Path(args[0]));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(DBOutputFormat.class);

        //add db jar
        String jarPath = "/pip/libs/db/mysql-connector-java-8.0.19.jar";
        job.addArchiveToClassPath(new Path(jarPath));
        DistributedCache.addFileToClassPath(new Path(jarPath), conf);

        //DB输出
        DBOutputFormat.setOutput(job, "test", "word", "times");

        job.waitForCompletion(true);


    }

}
