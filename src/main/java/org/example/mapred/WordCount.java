package org.example.mapred;

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
import java.util.StringTokenizer;

/**
 * Map对文件或目录中的文件中的单词进行次数统计，每次读一行，单词为key，1为value
 * Reduce进行相同单词求和
 */
public class WordCount {
    /**
     * Map函数
     * 第一个参数是Object，也可以写成Long，对应行偏移量
     * 第二个参数Text类型，Hadoop实现的String类型的可写类型，对应每行字符串
     * 第三个参数输出key的数据类型
     * 第四个参数输出value的数据类型
     */
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

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
        protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer((value.toString()));
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());//转换字符串到Text类型
                context.write(word, one);//写入到本地文件
            }
        }
    }


    /**
     * Reduce函数
     * 第一个参数是输入key数据类型
     * 第二个参数是输入value数据类型
     * 第三个参数输出key的数据类型，和setOutputKeyClass保持一致
     * 第四个参数输出value的数据类型，和setOutputValueClass保持一致
     */
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();


        /**
         * key是单词，value是出现频率列表
         *
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();

            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();

        //在hadoop集群中可以不用
        conf.set("fs.defaultFS", "hdfs://10.211.55.4:9000");
        System.setProperty("HADOOP_USER_NAME", "parallels");
        //在hadoop集群中可以不用 end

        Job job = Job.getInstance(conf, "Word count");

        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class); //Mapper
        job.setReducerClass(IntSumReducer.class); //Reduce

        job.setOutputKeyClass(Text.class); //Output key
        job.setOutputValueClass(IntWritable.class); //Output value

        FileInputFormat.addInputPath(job, new Path(args[0])); //Input folder, must exist
        FileOutputFormat.setOutputPath(job, new Path(args[1]));//Output folder, must not exist
        job.waitForCompletion(true); //Run
    }

}
