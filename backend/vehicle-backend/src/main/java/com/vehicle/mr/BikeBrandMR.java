package com.vehicle.mr;

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

public class BikeBrandMR {
    // Map阶段：输出<品牌, 1>
    public static class BrandMapper extends Mapper<Object, Text, Text, IntWritable> {
        private static final IntWritable ONE = new IntWritable(1);
        private Text brand = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length >= 6) {
                brand.set(fields[1]); // 假设品牌在第二列
                context.write(brand, ONE);
            }
        }
    }

    // Reduce阶段：统计品牌车辆总数
    public static class BrandReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable val : values) {
                count += val.get();
            }
            result.set(count);
            context.write(key, result);
        }
    }

    // 提交任务
    public void submitJob(String inputPath, String outputPath) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.220.151:8020");

        Path outPath = new Path(outputPath);
        org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        Job job = Job.getInstance(conf, "Bike Brand Count");
        job.setJarByClass(BikeBrandMR.class);
        job.setMapperClass(BrandMapper.class);
        job.setCombinerClass(BrandReducer.class); // 局部合并优化
        job.setReducerClass(BrandReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, outPath);

        boolean success = job.waitForCompletion(true);
        if (!success) {
            throw new Exception("品牌单车数量统计任务失败");
        }
    }
}