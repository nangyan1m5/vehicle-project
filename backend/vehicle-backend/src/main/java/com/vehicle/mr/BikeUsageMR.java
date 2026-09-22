package com.vehicle.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class BikeUsageMR {
    // Map阶段：精准解析第六列使用时间
    public static class UsageMapper extends Mapper<Object, Text, Text, Text> {
        private final Text brand = new Text();
        private final Text usageTime = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 空行过滤
            if (value == null || value.toString().trim().isEmpty()) {
                System.err.println("UsageMapper: 过滤空行数据");
                return;
            }

            String line = value.toString().trim();
            // 仅去除单引号（日志中数据带单引号，无其他特殊字符）
            String cleanLine = line.replace("'", "").trim();
            // 直接用逗号分割
            String[] fields = cleanLine.split(",");

            // 关键日志：打印分割结果
            System.out.println("UsageMapper: 原始行=[" + line + "]，清理后=[" + cleanLine + "]，字段数=" + fields.length);

            // 校验字段数（至少为6列）
            if (fields.length < 6) {
                System.err.println("UsageMapper: 过滤字段数异常数据，字段数=" + fields.length + "，行内容=" + line);
                return;
            }

            // 提取第二列品牌（索引1）和第六列使用时间（索引5）
            String brandStr = fields[1].trim();
            String usageTimeStr = fields[5].trim();

            // 过滤无效数据（品牌非空+使用时间为数字）
            if (brandStr.isEmpty() || !usageTimeStr.matches("\\d+")) {
                System.err.println("UsageMapper: 过滤无效数据，品牌=" + brandStr + "，使用时间=" + usageTimeStr);
                return;
            }

            // 输出键值对（品牌，使用时间）
            brand.set(brandStr);
            usageTime.set(usageTimeStr);
            context.write(brand, usageTime);
            System.out.println("UsageMapper: 输出 <" + brandStr + ", " + usageTimeStr + ">");
        }
    }

    // Reduce阶段：计算平均使用时间（容错处理）
    public static class UsageReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int totalTime = 0;
            int validCount = 0;

            for (Text val : values) {
                try {
                    int time = Integer.parseInt(val.toString().trim());
                    totalTime += time;
                    validCount++;
                    System.out.println("UsageReducer: 品牌=" + key + "，累加时间=" + time + "，累计=" + totalTime);
                } catch (NumberFormatException e) {
                    System.err.println("UsageReducer: 转换失败，品牌=" + key + "，无效值=" + val);
                }
            }

            if (validCount > 0) {
                int avgTime = totalTime / validCount;
                context.write(key, new Text(String.valueOf(avgTime)));
                System.out.println("UsageReducer: 品牌=" + key + "，平均使用时间=" + avgTime);
            } else {
                System.err.println("UsageReducer: 品牌=" + key + " 无有效数据");
            }
        }
    }

    // 提交任务
    public void submitJob(String inputPath, String outputPath) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.220.151:8020");

        // 删除已存在的输出目录
        Path outPath = new Path(outputPath);
        org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(conf);
        if (fs.exists(outPath)) {
            boolean deleteOk = fs.delete(outPath, true);
            System.out.println("UsageMR: 删除输出目录 " + outputPath + "：" + (deleteOk ? "成功" : "失败"));
        }

        // 配置Job
        Job job = Job.getInstance(conf, "Bike Average Usage Time");
        job.setJarByClass(BikeUsageMR.class);
        job.setMapperClass(UsageMapper.class);
        job.setReducerClass(UsageReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 输入输出路径
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        // 执行任务
        System.out.println("UsageMR: 开始执行，输入路径=" + inputPath + "，输出路径=" + outputPath);
        boolean success = job.waitForCompletion(true);
        if (!success) {
            throw new Exception("BikeUsageMR任务执行失败");
        }
        System.out.println("UsageMR: 执行成功，结果输出至=" + outputPath);
    }
}