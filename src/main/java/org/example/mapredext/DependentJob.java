package org.example.mapredext;

import org.apache.commons.lang3.StringUtils;
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.StringTokenizer;


/**
 * 依赖式MapReduce，job依赖job1和job2，输出job1和job2的共同结果
 */
public class DependentJob extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();

        return run(args, conf);
    }

    private static int run(String[] args, Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
        Job job1 = Job.getInstance(conf, "job1");
        job1.setJarByClass(DependentJob.class);
        job1.setMapperClass(TokenizerMapper.class);
        job1.setReducerClass(IntSumReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        Job job2 = Job.getInstance(conf, "job2");
        job2.setJarByClass(DependentJob.class);
        job2.setMapperClass(TokenizerMapper2.class);
        job2.setReducerClass(IntSumReducer2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job2, new Path(args[0])); //use reduce1's output
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));


        Job job3 = Job.getInstance(conf, "job3");
        job3.setJarByClass(DependentJob.class);
        job3.setMapperClass(TokenizerMapper3.class);
        job3.setReducerClass(IntSumReducer3.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job3, new Path(args[1])); //use reduce1's output
        FileInputFormat.addInputPath(job3, new Path(args[2])); //use reduce2's output
        FileOutputFormat.setOutputPath(job3, new Path(args[3]));


        //构造一个ControlledJob
        ControlledJob cjob1 = new ControlledJob(conf);
        cjob1.setJob(job1);
        ControlledJob cjob2 = new ControlledJob(conf);
        cjob2.setJob(job2);
        ControlledJob cjob3 = new ControlledJob(conf);
        cjob3.setJob(job3);

        cjob3.addDependingJob(cjob1);
        cjob3.addDependingJob(cjob2);

        JobControl dependJobControl = new JobControl("depend job");
        dependJobControl.addJob(cjob1);
        dependJobControl.addJob(cjob2);
        dependJobControl.addJob(cjob3);

        Thread t = new Thread(dependJobControl);
        t.start();
        while (true) {
            if (dependJobControl.allFinished()) {
                System.out.println(dependJobControl.getSuccessfulJobList());
                dependJobControl.stop();
                return 0;
            }
            if (dependJobControl.getFailedJobList().size() > 0) {
                System.out.println(dependJobControl.getFailedJobList());
                dependJobControl.stop();
                return 1;
            }
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

    public static class TokenizerMapper2 extends Mapper<LongWritable, Text, Text, LongWritable> {

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


    public static class IntSumReducer2 extends Reducer<Text, LongWritable, Text, LongWritable> {
        private IntWritable result = new IntWritable();


        //out key是"step2_"+word，out value是次数
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();


            }
            context.write(new Text("step2_" + key.toString()), new LongWritable(sum));
        }
    }

    public static class TokenizerMapper3 extends Mapper<LongWritable, Text, Text, LongWritable> {

        private final static LongWritable one = new LongWritable(1);
        private Text word = new Text();


        //key是"step3_"+word，value是value
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            if (value != null) {
                String line = value.toString();
                String[] split = StringUtils.split(line);
                context.write(new Text("step3_+" + split[0]), new LongWritable(Long.parseLong(split[1])));
            }
        }
    }

    public static class IntSumReducer3 extends Reducer<Text, LongWritable, Text, LongWritable> {
        private IntWritable result = new IntWritable();


        //out key是word，out value是不等于1的次数
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            for (LongWritable value : values) {
                if (value.get() != 1) {
                    context.write(key, value);
                }
            }
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
        Path outputDir2 = new Path(args[2]);
        if (fs.exists(outputDir2)) {
            fs.delete(outputDir2, true);
        }

        Path outputDir3 = new Path(args[3]);
        if (fs.exists(outputDir3)) {
            fs.delete(outputDir3, true);
        }

        run(args, conf);
    }

}
