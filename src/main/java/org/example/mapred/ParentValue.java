package org.example.mapred;

import org.apache.commons.compress.utils.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.List;

/**
 * 孙->爷关系输出
 * 输入为，子->父关系，经过转换输出孙->爷关系
 */
public class ParentValue {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            String child = value.toString().split(" ")[0];
            String parent = value.toString().split(" ")[1];

            //转换加特殊标记转换成父->子，子->父两种key，value
            context.write(new Text(child), new Text("-" + parent));
            context.write(new Text(parent), new Text("+" + child));
        }
    }

    public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            //保存当前人的父->子，子->父到List
            List<Text> grandparent = Lists.newArrayList();
            List<Text> grandchild = Lists.newArrayList();
            for (Text value : values) {

                String s = value.toString();
                if (s.startsWith("-")) {
                    grandparent.add(new Text(s));
                } else {
                    grandchild.add(new Text(s));
                }
            }
            //获取当前人的子，再获取当前人的父，则形成孙->爷关系
            for (int i = 0; i < grandchild.size(); i++) {
                for (int j = 0; j < grandparent.size(); j++) {
                    context.write(grandchild.get(i), grandparent.get(j));
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();

        conf.set("fs.defaultFS", "hdfs://10.211.55.4:9000");
        System.setProperty("HADOOP_USER_NAME", "parallels");

        Job job = Job.getInstance(conf, "Parent value");

        job.setJarByClass(ParentValue.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }

}
