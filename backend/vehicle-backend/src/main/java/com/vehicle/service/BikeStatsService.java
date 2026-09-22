package com.vehicle.service;

import com.vehicle.mapper.BikeStatsMapper;
import com.vehicle.mr.*;
import com.vehicle.pojo.BikeStats;
import com.vehicle.util.HdfsUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.*;

/**
 * 共享单车统计服务层
 * 功能：共享单车数据统计分析 + HDFS数据管理
 */
@Service
public class BikeStatsService {
    @Autowired
    private BikeStatsMapper bikeStatsMapper;

    @Value("${hdfs.input.path}")
    private String hdfsInputPath;

    @Value("${hdfs.output.path}")
    private String hdfsOutputPath;

    // 核心：单例复用 HDFS FileSystem 对象
    private FileSystem hdfsFileSystem;

    /**
     * 初始化 HDFS 连接（服务启动时执行）
     */
    @PostConstruct
    public void initHdfs() {
        try {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://192.168.220.151:8020");
            conf.setBoolean("fs.hdfs.impl.disable.cache", true);
            this.hdfsFileSystem = FileSystem.get(conf);
            HdfsUtil.setFileSystem(hdfsFileSystem);
        } catch (IOException e) {
            throw new RuntimeException("HDFS 连接初始化失败", e);
        }
    }

    /**
     * 1. 区域单车分布（调用MapReduce）
     */
    public Map<String, Integer> getRegionDistribution() throws Exception {
        try {
            BikeRegionMR mr = new BikeRegionMR();
            String outputPath = hdfsOutputPath + "/region_dist";

            if (HdfsUtil.exists(outputPath)) {
                HdfsUtil.delete(outputPath);
            }
            mr.submitJob(hdfsInputPath, outputPath);

            List<BikeStats> resultList = HdfsUtil.readBikeResult(outputPath);
            Map<String, Integer> resultMap = new HashMap<>();

            if (resultList == null || resultList.isEmpty()) {
                resultMap.put("暂无数据", 0);
                return resultMap;
            }

            for (BikeStats stats : resultList) {
                if (stats.getRegion() != null && stats.getBikeId() != null) {
                    resultMap.put(stats.getRegion(), Integer.parseInt(stats.getBikeId()));
                }
            }
            return resultMap;
        } catch (Exception e) {
            throw new Exception("获取区域分布数据失败：" + e.getMessage(), e);
        }
    }

    /**
     * 2. 区域平均骑行里程（调用MapReduce）
     */
    public Map<String, Double> getAverageMileageByRegion() throws Exception {
        try {
            BikeMileageMR mr = new BikeMileageMR();
            String outputPath = hdfsOutputPath + "/avg_mileage";

            if (HdfsUtil.exists(outputPath)) {
                HdfsUtil.delete(outputPath);
            }
            mr.submitJob(hdfsInputPath, outputPath);

            List<BikeStats> resultList = HdfsUtil.readBikeResult(outputPath);
            Map<String, Double> resultMap = new HashMap<>();

            if (resultList == null || resultList.isEmpty()) {
                resultMap.put("暂无数据", 0.0);
                return resultMap;
            }

            for (BikeStats stats : resultList) {
                if (stats.getRegion() != null && stats.getTotalMileage() != null) {
                    resultMap.put(stats.getRegion(), stats.getTotalMileage());
                }
            }
            return resultMap;
        } catch (Exception e) {
            throw new Exception("获取区域平均骑行里程失败：" + e.getMessage(), e);
        }
    }

    /**
     * 3. 品牌单车数量统计（调用MapReduce）
     */
    public Map<String, Integer> getBrandCount() {
        try {
            BikeBrandMR mr = new BikeBrandMR();
            String outputPath = hdfsOutputPath + "/brand_count";

            if (HdfsUtil.exists(outputPath)) {
                HdfsUtil.delete(outputPath);
            }
            mr.submitJob(hdfsInputPath, outputPath);

            List<BikeStats> resultList = HdfsUtil.readBikeResult(outputPath);
            Map<String, Integer> resultMap = new HashMap<>();

            if (resultList == null || resultList.isEmpty()) {
                resultMap.put("暂无数据", 0);
                return resultMap;
            }

            for (BikeStats stats : resultList) {
                if (stats.getBrand() != null && stats.getBikeId() != null) {
                    resultMap.put(stats.getBrand(), Integer.parseInt(stats.getBikeId()));
                }
            }
            return resultMap;
        } catch (Exception e) {
            Map<String, Integer> errorMap = new HashMap<>();
            errorMap.put("查询异常", 0);
            return errorMap;
        }
    }

    /**
     * 4. 投放年份分布（调用MapReduce）
     */
    public Map<Integer, Integer> getYearDistribution() {
        try {
            BikeYearMR mr = new BikeYearMR();
            String outputPath = hdfsOutputPath + "/year_dist";

            if (HdfsUtil.exists(outputPath)) {
                HdfsUtil.delete(outputPath);
            }
            mr.submitJob(hdfsInputPath, outputPath);

            List<BikeStats> resultList = HdfsUtil.readBikeResult(outputPath);
            Map<Integer, Integer> resultMap = new TreeMap<>();

            if (resultList == null || resultList.isEmpty()) {
                resultMap.put(0, 0);
                return resultMap;
            }

            for (BikeStats stats : resultList) {
                if (stats.getProductionYear() != null && stats.getBikeId() != null) {
                    resultMap.put(Integer.parseInt(stats.getProductionYear()), Integer.parseInt(stats.getBikeId()));
                }
            }
            return resultMap;
        } catch (Exception e) {
            Map<Integer, Integer> errorMap = new TreeMap<>();
            errorMap.put(0, 0);
            return errorMap;
        }
    }

    /**
     * 5. 品牌平均使用时长（调用MapReduce）
     */
    public Map<String, Integer> getAverageUsageTimeByBrand() throws Exception {
        try {
            BikeUsageMR mr = new BikeUsageMR();
            String outputPath = hdfsOutputPath + "/avg_usage_time";

            if (HdfsUtil.exists(outputPath)) {
                HdfsUtil.delete(outputPath);
            }
            mr.submitJob(hdfsInputPath, outputPath);

            List<BikeStats> resultList = HdfsUtil.readBikeResult(outputPath);
            Map<String, Integer> resultMap = new HashMap<>();

            if (resultList == null || resultList.isEmpty()) {
                resultMap.put("暂无数据", 0);
                return resultMap;
            }

            for (BikeStats stats : resultList) {
                if (stats.getBrand() != null && stats.getTotalUsageTime() != null) {
                    resultMap.put(stats.getBrand(), stats.getTotalUsageTime());
                }
            }
            return resultMap;
        } catch (Exception e) {
            throw new Exception("获取品牌平均使用时长失败：" + e.getMessage(), e);
        }
    }

    /**
     * 获取单车状态分布
     */
    public Map<String, Integer> getStatusDistribution() {
        try {
            List<Map<String, Object>> resultList = bikeStatsMapper.countByStatus();
            Map<String, Integer> resultMap = new HashMap<>();

            if (resultList == null || resultList.isEmpty()) {
                resultMap.put("暂无数据", 0);
                return resultMap;
            }

            for (Map<String, Object> map : resultList) {
                String status = (String) map.get("status");
                Integer count = ((Number) map.get("count")).intValue();
                resultMap.put(status, count);
            }
            return resultMap;
        } catch (Exception e) {
            Map<String, Integer> errorMap = new HashMap<>();
            errorMap.put("查询异常", 0);
            return errorMap;
        }
    }

    /**
     * 获取原始数据（用于数据可视化）
     */
    public List<Map<String, Object>> getRawData() {
        try {
            List<BikeStats> statsList = bikeStatsMapper.selectAll();
            List<Map<String, Object>> resultList = new ArrayList<>();

            if (statsList == null || statsList.isEmpty()) {
                Map<String, Object> emptyMap = new HashMap<>();
                emptyMap.put("brand", "暂无数据");
                emptyMap.put("model", "");
                emptyMap.put("totalUsageTime", 0);
                emptyMap.put("totalMileage", 0.0);
                resultList.add(emptyMap);
                return resultList;
            }

            for (BikeStats stats : statsList) {
                Map<String, Object> map = new HashMap<>();
                map.put("brand", stats.getBrand() == null ? "未知品牌" : stats.getBrand());
                map.put("model", stats.getModel() == null ? "未知型号" : stats.getModel());
                map.put("totalUsageTime", stats.getTotalUsageTime() == null ? 0 : stats.getTotalUsageTime());
                map.put("totalMileage", stats.getTotalMileage() == null ? 0.0 : stats.getTotalMileage());
                map.put("region", stats.getRegion() == null ? "未知区域" : stats.getRegion());
                map.put("status", stats.getStatus() == null ? "未知状态" : stats.getStatus());
                resultList.add(map);
            }
            return resultList;
        } catch (Exception e) {
            List<Map<String, Object>> errorList = new ArrayList<>();
            Map<String, Object> errorMap = new HashMap<>();
            errorMap.put("brand", "查询异常");
            errorMap.put("totalUsageTime", 0);
            errorMap.put("totalMileage", 0.0);
            errorMap.put("errorMsg", "获取原始数据失败：" + e.getMessage());
            errorList.add(errorMap);
            return errorList;
        }
    }

    /**
     * 销毁 HDFS 连接（服务关闭时执行）
     */
    @PreDestroy
    public void closeHdfs() {
        if (this.hdfsFileSystem != null) {
            try {
                this.hdfsFileSystem.close();
                HdfsUtil.closeSingletonFileSystem();
            } catch (IOException e) {
                System.err.println("关闭 HDFS 连接失败：" + e.getMessage());
            }
        }
    }
}