package org.example.mapredext;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;


/**
 * 自定义InputFormat类，组合使用Partitioner和MultipleOutputs
 * Map的key的迭代分组使用GroupingComparatorClass
 * Map的value果的排序使用MySortComparatorClass
 */
public class CustomizeInputFormatSample {

    private static String TAB = "\t";
    private static String FAN = "fan";
    private static String FOLLOWERS = "followers";
    private static String MICROBLOGS = "microblogs";

    public static class Weibo implements WritableComparable<Object> {
        private int fans;
        private int followers;
        private int microblogs;

        public Weibo() {

        }

        public Weibo(int fans, int followers, int microblogs) {
            this.fans = fans;
            this.followers = followers;
            this.microblogs = microblogs;
        }


        public void set(int fans, int followers, int microblogs) {
            this.fans = fans;
            this.followers = followers;
            this.microblogs = microblogs;
        }

        @Override
        public int compareTo(Object o) {
            return 0;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeInt(this.fans);
            dataOutput.writeInt(this.followers);
            dataOutput.writeInt(this.microblogs);
        }


        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.fans = dataInput.readInt();
            this.followers = dataInput.readInt();
            this.microblogs = dataInput.readInt();
        }


        public int getFans() {
            return fans;
        }

        public void setFans(int fans) {
            this.fans = fans;
        }

        public int getFollowers() {
            return followers;
        }

        public void setFollowers(int followers) {
            this.followers = followers;
        }

        public int getMicroblogs() {
            return microblogs;
        }

        public void setMicroblogs(int microblogs) {
            this.microblogs = microblogs;
        }
    }

    public static class WeiboInputFormat extends FileInputFormat<Text, Weibo> {
        @Override
        public RecordReader<Text, Weibo> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            return new WeiboRecordReader();

        }

        private class WeiboRecordReader extends RecordReader<Text, Weibo> {
            public LineReader in;
            public Text lineKey = new Text();
            public Weibo lineValue = new Weibo();

            @Override
            public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
                FileSplit split = (FileSplit) inputSplit;
                Configuration configuration = taskAttemptContext.getConfiguration();
                Path path = split.getPath();

                FileSystem fileSystem = path.getFileSystem(configuration);
                FSDataInputStream open = fileSystem.open(path);
                in = new LineReader(open);
            }

            @Override
            public boolean nextKeyValue() throws IOException, InterruptedException {
                Text line = new Text();
                int lineSize = in.readLine(line);
                if (lineSize == 0) {
                    return false;
                }

                String[] split = StringUtils.split(line.toString());
                if (split.length != 5) {
                    throw new IOException("Invalid record received " + line.toString());
                }

                int a = 0, b = 0, c = 0;
                try {
                    a = Integer.parseInt(split[2].trim());
                    b = Integer.parseInt(split[3].trim());
                    c = Integer.parseInt(split[4].trim());
                } catch (NumberFormatException numberFormatException) {
                    numberFormatException.printStackTrace();
                }

                lineKey.set(split[0] + "," + split[1]);
                lineValue.set(a, b, c);

                return true;
            }

            @Override
            public Text getCurrentKey() throws IOException, InterruptedException {
                return lineKey;
            }

            @Override
            public Weibo getCurrentValue() throws IOException, InterruptedException {
                return lineValue;
            }

            @Override
            public float getProgress() throws IOException, InterruptedException {
                return 0;
            }

            @Override
            public void close() throws IOException {
                if (in != null) {
                    in.close();
                }
            }
        }
    }


    public static class TokenizerMapper extends Mapper<Text, Weibo, Text, Text> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();


        //input key是（序号+逗号+名字）,value是包括Weibo数的对象
        //out key是partitioner key，value是 序号+逗号+名字+tab+数字
        @Override
        protected void map(Text key, Weibo value, Mapper<Text, Weibo, Text, Text>.Context context) throws IOException, InterruptedException {
            context.write(new Text(FAN), new Text(key.toString() + TAB + value.getFans()));
            context.write(new Text(FOLLOWERS), new Text(key.toString() + TAB + value.getFollowers()));
            context.write(new Text(MICROBLOGS), new Text(key.toString() + TAB + value.getMicroblogs()));
        }
    }


    public static class IntSumReducer extends Reducer<Text, Text, Text, IntWritable> {

        private MultipleOutputs<Text, IntWritable> multipleOutputs;

        @Override
        protected void setup(Reducer<Text, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            multipleOutputs = new MultipleOutputs<Text, IntWritable>(context);
        }

        @Override
        protected void cleanup(Reducer<Text, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            multipleOutputs.close();
        }

        //input key是分组，input value是名字（序号+逗号+名字）+tab+数字
        //out key是序号+逗号+名字，out value是数字
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {

            HashMap<String, Integer> map = new HashMap<>();

            StringBuffer sb = new StringBuffer();
            for (Text value : values) {
                String[] split = value.toString().split(TAB);
                map.put(split[0], Integer.parseInt(split[1]));
            }

            Map.Entry<String, Integer>[] sortedHashtableByValue = getSortedHashtableByValue(map);
            for (Map.Entry<String, Integer> stringIntegerEntry : sortedHashtableByValue) {
                multipleOutputs.write(key.toString(), stringIntegerEntry.getKey(), stringIntegerEntry.getValue());
            }

        }
    }

    public static Map.Entry<String, Integer>[] getSortedHashtableByValue(Map<String, Integer> h) {
        Map.Entry<String, Integer>[] entries = h.entrySet().toArray(new Map.Entry[0]);
        Arrays.sort(entries, new Comparator<Map.Entry<String, Integer>>() {
            @Override
            public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        });
        return entries;

    }


    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();

        //在hadoop集群中可以不用
        conf.set("fs.defaultFS", "hdfs://10.211.55.4:9000");
        System.setProperty("HADOOP_USER_NAME", "parallels");
        //在hadoop集群中可以不用 end


        Job job = Job.getInstance(conf, "Customize InputFormat sample");
        job.setJarByClass(CustomizeInputFormatSample.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);


        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class); //?
        job.setOutputValueClass(IntWritable.class);

        //delete output path
        Path outputDir = new Path(args[1]);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputDir)) {
            fs.delete(outputDir, true);
        }

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));//Output folder, must not exist

        //
        job.setInputFormatClass(WeiboInputFormat.class);
        MultipleOutputs.addNamedOutput(job, FAN, TextOutputFormat.class, Text.class, IntWritable.class);
        MultipleOutputs.addNamedOutput(job, FOLLOWERS, TextOutputFormat.class, Text.class, IntWritable.class);
        MultipleOutputs.addNamedOutput(job, MICROBLOGS, TextOutputFormat.class, Text.class, IntWritable.class);

        //去掉默认生成的part-r-00000文件
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);


        job.waitForCompletion(true);


    }

}
