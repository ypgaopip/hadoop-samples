package org.example.mapredext;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * 自定义比较器，相当于先根据名字分组，然后把每组的数字，排序逗号分隔展示
 * Map的key的迭代分组使用GroupingComparatorClass
 * Map的value果的排序使用MySortComparatorClass
 */
public class MyComparatorSample {

    public static class MySort implements WritableComparable<MySort> {
        private String first;
        private int second;

        public MySort() {

        }

        public MySort(String first, int second) {
            this.first = first;
            this.second = second;
        }

        @Override
        public int compareTo(MySort o) {
            return 0;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(this.first);
            dataOutput.writeInt(this.second);
        }


        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.first = dataInput.readUTF();
            this.second = dataInput.readInt();
        }

        public String getFirst() {
            return first;
        }

        public void setFirst(String first) {
            this.first = first;
        }

        public int getSecond() {
            return second;
        }

        public void setSecond(int second) {
            this.second = second;
        }
    }

    public static class MySortComparator extends WritableComparator {

        public MySortComparator() {
            super(MySort.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            MySort a1 = (MySort) a;
            MySort b1 = (MySort) b;

            if (!a1.getFirst().equals(b1.getFirst())) {
                return a1.getFirst().compareTo(b1.getFirst());
            } else {
                return -(a1.getSecond() - b1.getSecond());
            }
        }
    }

    public static class MyGroupingComparator extends WritableComparator {
        public MyGroupingComparator() {
            super(MySort.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            MySort a1 = (MySort) a;
            MySort b1 = (MySort) b;
            return a1.getFirst().compareTo(b1.getFirst());
        }
    }

    public static class TokenizerMapper extends Mapper<LongWritable, Text, MySort, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();


        //key是sort对象，value是数字
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, MySort, IntWritable>.Context context) throws IOException, InterruptedException {
            if (value != null) {
                String line = value.toString().trim();
                if (line.length() > 0) {
                    String[] split = line.split(",");
                    if (split.length == 3) {
                        one.set(Integer.parseInt(split[2]));
                        context.write(new MySort(split[1], Integer.parseInt(split[2])), one);
                    }
                }
            }
        }
    }


    public static class IntSumReducer extends Reducer<MySort, IntWritable, Text, Text> {

        // should be initialized, otherwise will NPE
        private Text outkey = new Text();
        private Text outvalue = new Text();

        //out key是name，out value是逗号组合的value
        @Override
        protected void reduce(MySort key, Iterable<IntWritable> values, Reducer<MySort, IntWritable, Text, Text>.Context context) throws IOException, InterruptedException {

            StringBuffer sb = new StringBuffer();
            for (IntWritable value : values) {
                sb.append(",");
                sb.append(value);
            }
            sb.delete(0, 1);
            outkey.set(key.getFirst());
            outvalue.set(sb.toString());
            context.write(outkey, outvalue);
        }
    }


    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();

        //在hadoop集群中可以不用
        conf.set("fs.defaultFS", "hdfs://10.211.55.4:9000");
        System.setProperty("HADOOP_USER_NAME", "parallels");
        //在hadoop集群中可以不用 end


        Job job = Job.getInstance(conf, "My comparator sample");
        job.setJarByClass(MyComparatorSample.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setMapOutputKeyClass(MySort.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置排序比较器
        job.setSortComparatorClass(MySortComparator.class);
        //设置分组比较器
        job.setGroupingComparatorClass(MyGroupingComparator.class);

        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class); //?
        job.setOutputValueClass(Text.class);

        //delete output path
        Path outputDir = new Path(args[1]);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputDir)) {
            fs.delete(outputDir, true);
        }

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));//Output folder, must not exist

        job.waitForCompletion(true);


    }

}
