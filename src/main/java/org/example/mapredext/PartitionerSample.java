package org.example.mapredext;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


/**
 * 增加分区处理，需要事先知道有多少个分区，输出到多个文件，自动序号增加
 * Combiner, must be ignored while using Partitioner
 */
public class PartitionerSample {

    public static class ScorePartitioner extends Partitioner<IntWritable, Text> {

        /**
         * Key是分数
         *
         * @param intWritable
         * @param text
         * @param numberReduceTasks
         * @return
         */
        @Override
        public int getPartition(IntWritable intWritable, Text text, int numberReduceTasks) {

            int score = intWritable.get();
            if (numberReduceTasks == 0) {
                return 0;
            }
            if (score < 60) {
                return 0;
            } else if (score <= 80) {
                return 1;
            } else {
                return 2;
            }
        }
    }


    public static class TokenizerMapper extends Mapper<Object, Text, IntWritable, Text> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();


        /**
         * key是行偏移量，value是每行字符串
         *
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {
            String[] line = StringUtils.split(value.toString());
            IntWritable scoreKey = new IntWritable(Integer.parseInt(line[1].trim()));
            context.write(scoreKey, value);
        }
    }


    /**
     * Reduce函数
     * 第一个参数是输入key数据类型
     * 第二个参数是输入value数据类型
     * 第三个参数输出key的数据类型，和setOutputKeyClass保持一致
     * 第四个参数输出value的数据类型，和setOutputValueClass保持一致
     */
    public static class IntSumReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
        private IntWritable result = new IntWritable();


        /**
         * key是分数，value是行内容
         *
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Reducer<IntWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (Text value : values) {
                context.write(value, null);

            }
        }

    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();

        //在hadoop集群中可以不用
        conf.set("fs.defaultFS", "hdfs://10.211.55.4:9000");
        System.setProperty("HADOOP_USER_NAME", "parallels");
        //在hadoop集群中可以不用 end

        Job job = Job.getInstance(conf, "Partitioner sample");

        job.setJarByClass(PartitionerSample.class);
        job.setMapperClass(TokenizerMapper.class); //Mapper
        job.setReducerClass(IntSumReducer.class); //Reduce
//        job.setCombinerClass(IntSumReducer.class); // Combiner, must be ignored while using Partitioner

        job.setMapOutputKeyClass(IntWritable.class); //Output key
        job.setMapOutputValueClass(Text.class); //Output value

        job.setOutputKeyClass(Text.class); //Output key
        job.setOutputValueClass(IntWritable.class); //Output value

        //delete output path
        Path outputDir = new Path(args[1]);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputDir)) {
            fs.delete(outputDir, true);
        }

        FileInputFormat.addInputPath(job, new Path(args[0])); //Input folder, must exist
        FileOutputFormat.setOutputPath(job, new Path(args[1]));//Output folder, must not exist

        //set partitioner info
        job.setPartitionerClass(ScorePartitioner.class);
        job.setNumReduceTasks(3);
        //set partitioner info end
        job.waitForCompletion(true); //Run
    }

}
