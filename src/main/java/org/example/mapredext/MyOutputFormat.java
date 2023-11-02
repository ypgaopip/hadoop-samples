package org.example.mapredext;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.StringTokenizer;


/**
 * 指定成功文件路径和数据文件路径，用FSDataOutputStream输出，继承OutputFormat类
 * 通过hdfs dfs -text /pip/extreduce/03-2/customer_output查看结果
 * 生成文件是二进制格式
 */
public class MyOutputFormat {


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


    public static class IntSumReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        private IntWritable result = new IntWritable();


        //out key是word，out value是次数
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }

    public static class MyselfOutputFormat extends OutputFormat<Text, LongWritable> {

        private FSDataOutputStream outputStream;
        public static String successPath;
        private static String outputPath;

        @Override
        public RecordWriter<Text, LongWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            FileSystem fileSystem = null;
            try {
                fileSystem = FileSystem.get(new URI(outputPath), taskAttemptContext.getConfiguration());
            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
            // output data file path
            Path path = new Path(outputPath);
            this.outputStream = fileSystem.create(path, false);

            return new MyRecordWriter(outputStream);
        }

        @Override
        public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {

        }

        @Override
        public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            // success file path
            return new FileOutputCommitter(new Path(successPath), taskAttemptContext);
        }
    }

    public static class MyRecordWriter extends RecordWriter<Text, LongWritable> {
        private FSDataOutputStream outputStream;

        public MyRecordWriter(FSDataOutputStream outputStream) {
            this.outputStream = outputStream;
        }

        @Override
        public void write(Text text, LongWritable longWritable) throws IOException, InterruptedException {
            // output data, but the data can no be seen by big data plugin
            this.outputStream.writeBytes(text.toString());
            this.outputStream.writeBytes("\t\r\n");
            this.outputStream.writeLong(longWritable.get());
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            this.outputStream.close();
        }
    }


    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();

        //在hadoop集群中可以不用
        conf.set("fs.defaultFS", "hdfs://10.211.55.4:9000");
        System.setProperty("HADOOP_USER_NAME", "parallels");
        //在hadoop集群中可以不用 end

        Job job = Job.getInstance(conf, "My output format");
        job.setJarByClass(MyOutputFormat.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //delete output path
        Path outputDir = new Path(args[1]);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputDir)) {
            fs.delete(outputDir, true);
        }
        Path outputDir2 = new Path(args[2]);
        if (fs.exists(outputDir2)) {
            fs.delete(outputDir2, true);
        }


        FileInputFormat.addInputPath(job, new Path(args[0]));
        // set customer output format path and class
        MyselfOutputFormat.successPath = args[1];
        MyselfOutputFormat.outputPath = args[2];
        job.setOutputFormatClass(MyselfOutputFormat.class);
        // set customer output format path and class end

        job.waitForCompletion(true);
    }

}
