package org.example.mapredext;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;


/**
 * 根据次数，指定输出文件名前缀，输出到多个文件中里面，还有一个默认到part-r-00000的文件（内容为空）
 */
public class MultipleOutputsSample {


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

        private MultipleOutputs<Text, LongWritable> multipleOutputs;


        @Override
        protected void setup(Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            multipleOutputs = new MultipleOutputs<Text, LongWritable>(context);
        }

        //out key是word，out value是次数
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();

            }
            // 第三个参数是输出文件名，为次数
            multipleOutputs.write(key, new LongWritable(sum), String.valueOf(sum));
        }

        @Override
        protected void cleanup(Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            multipleOutputs.close();
        }
    }


    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();

        //在hadoop集群中可以不用
        conf.set("fs.defaultFS", "hdfs://10.211.55.4:9000");
        System.setProperty("HADOOP_USER_NAME", "parallels");
        //在hadoop集群中可以不用 end

        Job job = Job.getInstance(conf, "Multiple outputs sample");
        job.setJarByClass(MultipleOutputsSample.class);

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

        //去掉默认生成的part-r-00000文件
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

        job.waitForCompletion(true);
    }

}
