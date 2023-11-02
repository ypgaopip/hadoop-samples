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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.StringTokenizer;


/**
 * 输出文件格式为XML格式，继承FileOutputFormat类
 */
public class MyXMLOutputFormat {


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

    public static class MyselfOutputFormat extends FileOutputFormat<Text, LongWritable> {

        @Override
        public RecordWriter<Text, LongWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            Path defaultWorkFile = getDefaultWorkFile(taskAttemptContext, "");
            FileSystem fileSystem = FileSystem.get(taskAttemptContext.getConfiguration());
            FSDataOutputStream fsDataOutputStream = fileSystem.create(defaultWorkFile, false);
            return new MyRecordWriter(fsDataOutputStream);
        }
    }

    public static class MyRecordWriter extends RecordWriter<Text, LongWritable> {

        private DataOutputStream out;

        public MyRecordWriter(DataOutputStream out) throws IOException {
            this.out = out;
            this.out.writeBytes("<xml>\n");
        }

        private void writeTag(String tag, String value) throws IOException {
            if ("content".equalsIgnoreCase(tag)) {
                out.writeBytes("\t<" + tag + ">" + value + "</" + tag + ">\n");
            } else {
                out.writeBytes("\t<" + tag + ">" + value + "</" + tag + ">\n\t");
            }
        }

        @Override
        public void write(Text text, LongWritable longWritable) throws IOException, InterruptedException {
            // output data, but the data can no be seen by big data plugin
            out.writeBytes("\t<data>\n\t");
            this.writeTag("key", text.toString());
            this.writeTag("content", longWritable.toString());
            out.writeBytes("\t</data>\n");
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            this.out.writeBytes("</xml>\n");
            this.out.close();
        }
    }


    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();

        //在hadoop集群中可以不用
        conf.set("fs.defaultFS", "hdfs://10.211.55.4:9000");
        System.setProperty("HADOOP_USER_NAME", "parallels");
        //在hadoop集群中可以不用 end

        Job job = Job.getInstance(conf, "My xml output format");
        job.setJarByClass(MyXMLOutputFormat.class);

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


        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputFormatClass(MyselfOutputFormat.class);

        job.waitForCompletion(true);
    }

}
