package com.vehicle.util; // 核心修改：包名从 com.wordcount.util → com.vehicle.util，匹配车辆系统架构

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.stereotype.Component;

import java.io.InputStream;

/**
 * Hadoop 工具类（适配车辆大数据统计系统）
 * 功能：HDFS 文件上传、路径删除（支持 MR 任务输出目录清理）
 */
@Component
public class HadoopUtil {
    // HDFS配置（核心修改：地址改为车辆系统的 192.168.220.151，与 application.properties 一致）
    private static final Configuration conf = new Configuration();
    static {
        // 原 hive01 改为集群实际IP，避免主机名解析失败（测试环境优先用IP）
        conf.set("fs.defaultFS", "hdfs://192.168.220.151:8020");

        // 新增：解决 Windows 环境下 HDFS 权限问题（可选，测试环境用）
        conf.set("hadoop.security.authentication", "simple");
        System.setProperty("HADOOP_USER_NAME", "hive"); // 与 Hive 运行用户一致
    }

    // 上传文件到HDFS（保留核心逻辑，优化资源释放）
    public static void uploadFile(InputStream inputStream, String hdfsPath) throws Exception {
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
            Path path = new Path(hdfsPath);

            // 覆盖已存在的文件（车辆系统导入数据时需覆盖旧文件）
            if (fs.exists(path)) {
                fs.delete(path, true);
            }

            try (FSDataOutputStream outputStream = fs.create(path)) {
                byte[] buffer = new byte[1024];
                int len;
                while ((len = inputStream.read(buffer)) != -1) {
                    outputStream.write(buffer, 0, len);
                }
                outputStream.hsync(); // 强制刷盘，确保文件写入完成
            }
        } finally {
            // 统一释放资源，避免流泄露
            if (inputStream != null) {
                inputStream.close();
            }
            if (fs != null) {
                fs.close();
            }
        }
    }

    // 删除HDFS路径（用于清理MR任务旧输出，优化异常处理）
    public static void deletePath(String hdfsPath) throws Exception {
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
            Path path = new Path(hdfsPath);
            if (fs.exists(path)) {
                boolean deleted = fs.delete(path, true); // 递归删除
                if (!deleted) {
                    throw new Exception("删除HDFS路径失败：" + hdfsPath);
                }
            }
        } finally {
            if (fs != null) {
                fs.close();
            }
        }
    }
}