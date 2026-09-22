package com.vehicle.util;

import com.vehicle.pojo.BikeStats;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * HDFS 工具类：包含基础操作 + MR 结果读取扩展方法
 * 修正点：移除对 VehicleStats 的依赖，统一使用 BikeStats
 */
public class HdfsUtil {
    // 核心：静态单例 FileSystem，避免重复创建/关闭
    private static FileSystem singletonFs;
    // HDFS 集群地址常量（统一配置，避免硬编码）
    private static final String HDFS_ADDR = "hdfs://192.168.220.151:8020";

    // ==================== 原有基础方法（仅微调，保留核心逻辑）====================
    /**
     * 原有 HDFS 路径判断方法（微调：复用单例连接）
     */
    public static boolean exists(String path) throws Exception {
        FileSystem fs = getSingletonFileSystem(); // 复用单例连接
        boolean exists = fs.exists(new Path(path));
        return exists;
    }

    /**
     * 读取 MapReduce 任务在 HDFS 上的输出结果（part-r-00000）- 用于单车统计
     */
    public static List<BikeStats> readBikeResult(String hdfsOutputPath) throws Exception {
        List<BikeStats> result = new ArrayList<>();
        FileSystem fs = getSingletonFileSystem();

        Path resultPath = new Path(hdfsOutputPath + "/part-r-00000");
        if (!fs.exists(resultPath)) {
            throw new Exception("MR 结果文件不存在：" + resultPath);
        }

        try (FSDataInputStream in = fs.open(resultPath);
             BufferedReader br = new BufferedReader(new InputStreamReader(in, "UTF-8"))) {

            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split("\t");
                if (parts.length == 2) {
                    BikeStats stats = new BikeStats();

                    if (hdfsOutputPath.contains("region_dist")) {
                        // 1. 地区分布 MR：key=地区，value=数量
                        stats.setRegion(parts[0]);
                        stats.setBikeId(parts[1]);
                    } else if (hdfsOutputPath.contains("avg_mileage")) {
                        // 2. 平均里程 MR：key=地区，value=里程
                        stats.setRegion(parts[0]);
                        try {
                            stats.setTotalMileage(Double.parseDouble(parts[1]));
                        } catch (NumberFormatException e) {
                            stats.setTotalMileage(0.0);
                            System.err.println("平均里程转换异常：" + line);
                        }
                    } else if (hdfsOutputPath.contains("avg_usage_time")) {
                        // 3. 平均使用时间 MR：key=品牌，value=时间
                        stats.setBrand(parts[0]);
                        try {
                            stats.setTotalUsageTime(Integer.parseInt(parts[1]));
                        } catch (NumberFormatException e) {
                            stats.setTotalUsageTime(0);
                            System.err.println("使用时间转换异常：" + line);
                        }
                    } else if (hdfsOutputPath.contains("brand_count")) {
                        // 4. 品牌数量 MR：key=品牌，value=数量
                        stats.setBrand(parts[0]);
                        stats.setBikeId(parts[1]);
                    } else if (hdfsOutputPath.contains("year_dist")) {
                        // 5. 年份分布 MR：使用 productionYear 字段
                        stats.setProductionYear(parts[0]); // 年份
                        try {
                            stats.setBikeId(parts[1]); // 数量
                        } catch (Exception e) {
                            stats.setBikeId("0");
                            System.err.println("年份分布转换异常：" + line);
                        }
                    }
                    result.add(stats);
                } else {
                    System.err.println("MR 结果格式错误：" + line);
                }
            }
        } catch (Exception e) {
            throw new Exception("读取 MR 结果失败：" + e.getMessage(), e);
        }
        return result;
    }

    /**
     * 原有 HDFS 文件删除方法（微调：复用单例连接）
     */
    public static boolean delete(String path) throws Exception {
        FileSystem fs = getSingletonFileSystem(); // 复用单例连接
        boolean deleted = fs.delete(new Path(path), true); // true 表示递归删除
        return deleted;
    }

    // ==================== 单例 FileSystem 管理方法 ====================
    /**
     * 获取单例 FileSystem 连接（懒加载，首次调用初始化）
     */
    private static FileSystem getSingletonFileSystem() throws Exception {
        if (singletonFs == null) {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", HDFS_ADDR);
            conf.setBoolean("fs.hdfs.impl.disable.cache", true); // 禁用缓存
            singletonFs = FileSystem.get(conf);
        }
        return singletonFs;
    }

    /**
     * 供外部注入 FileSystem（兼容服务类的 initHdfs 方法）
     */
    public static void setFileSystem(FileSystem fileSystem) {
        singletonFs = fileSystem;
    }

    /**
     * 手动关闭单例连接（服务销毁时调用）
     */
    public static void closeSingletonFileSystem() {
        if (singletonFs != null) {
            try {
                singletonFs.close();
                singletonFs = null;
            } catch (Exception e) {
                System.err.println("关闭 HDFS 单例连接失败：" + e.getMessage());
            }
        }
    }
}