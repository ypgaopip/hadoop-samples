package org.example.mapredext;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.StringTokenizer;


/**
 * 链式MapReduce
 * 你以为的这样：mapper1 和 mapper3 并行 -> reduce1 -> mapper2 -> mapper4
 * 实际上的这样：mapper1 -> mapper3 -> mapper2 -> mapper4 -> reduce1
 * 注意，只能有一个reduce
 */
public class ChainJob extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();

        return run(args, conf);
    }

    private static int run(String[] args, Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
        Job chainJob = Job.getInstance(conf, "chain job");

        chainJob.setInputFormatClass(TextInputFormat.class);
        chainJob.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(chainJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(chainJob, new Path(args[1]));

        // 只能有一个Reducer
        ChainMapper.addMapper(chainJob, TokenizerMapper.class, LongWritable.class, Text.class, Text.class, LongWritable.class, conf);
        ChainMapper.addMapper(chainJob, TokenizerMapper3.class, Text.class, LongWritable.class, Text.class, LongWritable.class, conf);
        // 虽然reduce的位置在这里，不代表reduce是第三个执行的
        // 但是mapper的执行和addMapper的顺序有关
        ChainReducer.setReducer(chainJob, IntSumReducer.class, Text.class, LongWritable.class, Text.class, LongWritable.class, conf);
        ChainMapper.addMapper(chainJob, TokenizerMapper2.class, Text.class, LongWritable.class, Text.class, LongWritable.class, conf);
        ChainMapper.addMapper(chainJob, TokenizerMapper4.class, Text.class, LongWritable.class, Text.class, LongWritable.class, conf);


        chainJob.waitForCompletion(true);
        return chainJob.isSuccessful() ? 0 : 1;

    }


    public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        private final static LongWritable one = new LongWritable(1);
        private Text word = new Text();


        //output key是"mapper1_"+word，value是1
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            if (value != null) {
                String line = value.toString();
                StringTokenizer stringTokenizer = new StringTokenizer(line);
                while (stringTokenizer.hasMoreTokens()) {
                    word.set("mapper1_" + stringTokenizer.nextToken());
                    context.write(word, one);
                }
            }
        }
    }


    public static class TokenizerMapper2 extends Mapper<Text, LongWritable, Text, LongWritable> {
        private final static LongWritable one = new LongWritable(1);
        private Text word = new Text();

        //output key是"mapper2_"+word，value是1
        @Override
        protected void map(Text key, LongWritable value, Mapper<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            context.write(new Text("mapper2_" + key.toString()), new LongWritable(value.get()));
        }
    }


    public static class IntSumReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        private IntWritable result = new IntWritable();


        //out key是"reduce_+word，out value是次数
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }
            context.write(new Text("reduce_" + key.toString()), new LongWritable(sum));
        }
    }

    public static class TokenizerMapper3 extends Mapper<Text, LongWritable, Text, LongWritable> {

        private final static LongWritable one = new LongWritable(1);
        private Text word = new Text();


        //key是"mapper3_"+word，value是value
        @Override
        protected void map(Text key, LongWritable value, Mapper<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            context.write(new Text("mapper3_" + key.toString()), new LongWritable(value.get()));
        }
    }


    public static class TokenizerMapper4 extends Mapper<Text, LongWritable, Text, LongWritable> {

        private final static LongWritable one = new LongWritable(1);
        private Text word = new Text();


        //key是"mapper4_"+word，value是value
        @Override
        protected void map(Text key, LongWritable value, Mapper<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            context.write(new Text("mapper4_" + key.toString()), new LongWritable(value.get()));
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();

        //在hadoop集群中可以不用
        conf.set("fs.defaultFS", "hdfs://10.211.55.4:9000");
        System.setProperty("HADOOP_USER_NAME", "parallels");
        //在hadoop集群中可以不用 end

        //delete output path
        Path outputDir = new Path(args[1]);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputDir)) {
            fs.delete(outputDir, true);
        }

        run(args, conf);
    }

}
