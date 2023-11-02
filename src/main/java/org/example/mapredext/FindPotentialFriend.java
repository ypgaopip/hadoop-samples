package org.example.mapredext;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.ArrayList;


/**
 * 利用Job嵌套求解二度人脉
 * 比如I认识C、G、H，但C不认识G，那么C-G就是一对潜在好友，但G-H早就认识了，因此不算为潜在好友。
 */
public class FindPotentialFriend {

    private static String TAB = "\t";


    //第一轮，得到A-B,B-A
    public static class Job1_Mapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context)

                throws IOException, InterruptedException {

            String[] line = StringUtils.split(value.toString());

            context.write(new Text(line[0]), new Text(line[1]));

            context.write(new Text(line[1]), new Text(line[0]));

        }

    }


    public static class Job1_Reducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)

                throws IOException, InterruptedException {

            ArrayList<String> potential_friends = new ArrayList<String>();

            // A，（B，C）里面 context写入，key为A-B，value为1，key为A-C，value为1
            for (Text v : values) {

                potential_friends.add(v.toString());

                //输出排序，为A-B，而不是B-A
                if (key.toString().compareTo(v.toString()) < 0) {
                    context.write(new Text(key + TAB + v), new Text("1"));
                } else {
                    context.write(new Text(v + TAB + key), new Text("1"));
                }
            }

            // A，（B，C）里面 B和C context写入，key为B-C，value为2
            //过滤掉 相同（B-B，C-C），和 顺序后大于前（C-B）
            for (int i = 0; i < potential_friends.size(); i++) {

                for (int j = 0; j < potential_friends.size(); j++) {

                    if (potential_friends.get(i).compareTo(

                            potential_friends.get(j)) < 0) {

                        context.write(new Text(potential_friends.get(i) + TAB

                                + potential_friends.get(j)), new Text("2"));

                    }

                }

            }

        }

    }

    public static class Job2_Mapper extends Mapper<Object, Text, Text, Text> {

        // reduce1的输出为map2的输入
        public void map(Object key, Text value, Context context)

                throws IOException, InterruptedException {

            String[] line = org.apache.commons.lang3.StringUtils.split(value.toString());
            context.write(new Text(line[0] + TAB + line[1]), new Text(line[2]));

        }

    }


    public static class Job2_Reducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)

                throws IOException, InterruptedException {

            //检查合并之后是否存在任意一对一度人脉，存在则不输出
            boolean is_potential_friend = true;

            for (Text v : values) {

                if (v.toString().equals("1")) {

                    is_potential_friend = false;

                    break;

                }

            }

            if (is_potential_friend) {

                String[] potential_friends = key.toString().split(TAB);

                context.write(new Text(potential_friends[0]), new Text(

                        potential_friends[1]));

            }

        }

    }


    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();

        //在hadoop集群中可以不用
        conf.set("fs.defaultFS", "hdfs://10.211.55.4:9000");
        System.setProperty("HADOOP_USER_NAME", "parallels");
        //在hadoop集群中可以不用 end

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 3) {
            System.err.println("Usage: hadoop jar <jarname> org.example.mapredext.FindPotentialFriend <in> <out_temp> <out>");
            System.exit(2);
        }

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

        Job job1 = Job.getInstance(conf, "Find potential friend map1");

        job1.setMapperClass(Job1_Mapper.class);
        job1.setReducerClass(Job1_Reducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));

        //等待完成
        if (job1.waitForCompletion(true)) {

            Job job2 = Job.getInstance(conf, "Find potential friend map2");

            job2.setMapperClass(Job2_Mapper.class);
            job2.setReducerClass(Job2_Reducer.class);

            FileInputFormat.addInputPath(job2, new Path(otherArgs[1]));
            FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));

            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);

            System.exit(job2.waitForCompletion(true) ? 0 : 1);

        }

    }

}
