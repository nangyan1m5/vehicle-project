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

public class BikeModelMR {
    // Map阶段：输出<品牌, 1>（统计每个品牌的计数）
    public static class ModelMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text brand = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length >= 6) {  // 校验数据完整性
                brand.set(fields[1]);  // brand字段（第2列）
                context.write(brand, one);
            }
        }
    }

    // Reduce阶段：汇总每个品牌的数量
    public static class ModelReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable val : values) {
                count += val.get();
            }
            context.write(key, new IntWritable(count));
        }
    }

    // 提交任务
    public void submitJob(String inputPath, String outputPath) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.220.151:8020");

        // 删除已存在的输出目录（避免任务报错）
        Path outPath = new Path(outputPath);
        org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        Job job = Job.getInstance(conf, "Bike Brand Count");
        job.setJarByClass(BikeModelMR.class);
        job.setMapperClass(ModelMapper.class);
        job.setReducerClass(ModelReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, outPath);

        boolean success = job.waitForCompletion(true);
        if (!success) {
            throw new Exception("品牌数量统计任务失败");
        }
    }
}