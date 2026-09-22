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
import java.math.BigDecimal;
import java.math.RoundingMode;

public class BikeMileageMR {
    // Map阶段：输出<地区, 里程>，增加小数点数值校验
    public static class MileageMapper extends Mapper<Object, Text, Text, Text> {
        private Text region = new Text();
        private Text mileage = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length >= 6) {  // 校验数据完整性
                String regionStr = fields[3].trim();
                String mileageStr = fields[4].trim();

                // 新增：校验地区非空 + 里程数值合法性（兼容小数点）
                if (!regionStr.isEmpty() && isNumeric(mileageStr)) {
                    region.set(regionStr);
                    mileage.set(mileageStr);
                    context.write(region, mileage);
                }
            }
        }

        // 工具方法：判断字符串是否为数值（兼容整数/小数点）
        private boolean isNumeric(String str) {
            if (str == null || str.isEmpty()) {
                return false;
            }
            try {
                Double.parseDouble(str);
                return true;
            } catch (NumberFormatException e) {
                return false;
            }
        }
    }

    // Reduce阶段：计算地区平均里程，修复小数点精度问题
    public static class MileageReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // 改用BigDecimal存储总里程，避免double精度丢失
            BigDecimal totalMileage = BigDecimal.ZERO;
            int count = 0;

            for (Text val : values) {
                String mileageStr = val.toString().trim();
                // 再次校验数值（双重保险）
                if (isNumeric(mileageStr)) {
                    BigDecimal mileage = new BigDecimal(mileageStr);
                    totalMileage = totalMileage.add(mileage);
                    count++;
                }
            }

            // 处理除零异常（避免无有效数据时崩溃）
            BigDecimal avgMileage = BigDecimal.ZERO;
            if (count > 0) {
                // 计算平均值，保留2位小数，四舍五入
                avgMileage = totalMileage.divide(
                        new BigDecimal(count),
                        2,  // 保留2位小数
                        RoundingMode.HALF_UP  // 四舍五入模式
                );
            }

            // 输出最终结果（兼容小数点）
            context.write(key, new Text(avgMileage.toString()));
        }

        // 复用数值校验方法
        private boolean isNumeric(String str) {
            if (str == null || str.isEmpty()) {
                return false;
            }
            try {
                Double.parseDouble(str);
                return true;
            } catch (NumberFormatException e) {
                return false;
            }
        }
    }

    // 提交任务（原有逻辑不变）
    public void submitJob(String inputPath, String outputPath) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.220.151:8020");

        // 删除已存在的输出目录（避免报错）
        Path outPath = new Path(outputPath);
        org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        Job job = Job.getInstance(conf, "Bike Average Mileage");
        job.setJarByClass(BikeMileageMR.class);
        job.setMapperClass(MileageMapper.class);
        job.setReducerClass(MileageReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, outPath);

        boolean success = job.waitForCompletion(true);
        if (!success) {
            throw new Exception("地区平均里程统计任务失败");
        }
    }
}