package org.example.mapredext;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


/**
 * 先分组，然后条件CASE处理，最后取聚集最大值
 * 先根据性别，然后根据年龄，然后取最高分
 */
public class GroupCaseMaxSample {

    private static String TAB = "\t";


    public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, Text> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();


        //key是gender，value是名字+年龄+分数
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            String[] split = StringUtils.split(value.toString().trim());
            String gender = split[2];
            String nameAgeScore = split[0] + TAB + split[1] + TAB + split[3];

            context.write(new Text(gender), new Text(nameAgeScore));
        }
    }


    public static class IntSumCombiner extends Reducer<Text, Text, Text, Text> {


        //input key是分组，input value是名字+数字
        //out key是name，out value是逗号组合的value

        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {

            int maxScore = Integer.MIN_VALUE;
            int score = 0;
            String name = "";
            String age = "";

            for (Text value : values) {
                String[] split = value.toString().split(TAB);
                score = Integer.parseInt(split[2]);
                if (score > maxScore) {
                    name = split[0];
                    age = split[1];
                    maxScore = score;
                }
            }
            //这个直接输出就是最高分的了
            context.write(key, new Text(name + TAB + age + TAB + maxScore));

        }
    }

    public static class MyPartitioner extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numberReduceTasks) {
            String[] split = StringUtils.split(value.toString().trim());
            int age = Integer.parseInt(split[1]);

            if (numberReduceTasks == 0) {
                return 0;
            }
            if (age < 20) {
                return 0;
            } else if (age <= 50) {
                return 1 % numberReduceTasks;
            } else {
                return 2 % numberReduceTasks;
            }
        }
    }


    public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {


        //input key是分组，input value是名字+数字
        //out key是name，out value是逗号组合的value
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {

            int maxScore = Integer.MIN_VALUE;
            int score = 0;
            String name = "";
            String age = "";
            String gender = "";

            for (Text value : values) {
                String[] split = value.toString().split(TAB);
                score = Integer.parseInt(split[2]);
                if (score > maxScore) {
                    name = split[0];
                    age = split[1];
                    gender = key.toString();
                    maxScore = score;
                }
            }
            context.write(new Text(name), new Text("age:" + age + TAB + "gender:" + gender
                    + TAB + "score:" + maxScore));

        }
    }


    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();

        //在hadoop集群中可以不用
        conf.set("fs.defaultFS", "hdfs://10.211.55.4:9000");
        System.setProperty("HADOOP_USER_NAME", "parallels");
        //在hadoop集群中可以不用 end


        Job job = Job.getInstance(conf, "Group Case Max Sample");
        job.setJarByClass(GroupCaseMaxSample.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);


        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class); //?
        job.setOutputValueClass(Text.class);

        job.setCombinerClass(IntSumCombiner.class);
        job.setPartitionerClass(MyPartitioner.class);
        job.setNumReduceTasks(3);

        //delete output path
        Path outputDir = new Path(args[1]);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputDir)) {
            fs.delete(outputDir, true);
        }

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);


    }

}
