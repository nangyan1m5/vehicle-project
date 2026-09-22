package com.vehicle.controller;

import com.vehicle.service.BikeStatsService;
import com.vehicle.util.HadoopUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.Map;
import java.util.List;
import java.util.HashMap;

/**
 * 共享单车统计控制器
 * 功能：跨域支持 + 异常兜底 + 全链路日志 + 数据上传功能
 */
@RestController
@RequestMapping("/api/bike")
// 精准允许前端地址
@CrossOrigin(origins = "http://localhost:8081", maxAge = 3600)
public class BikeStatsController {
    // SLF4J日志
    private static final Logger log = LoggerFactory.getLogger(BikeStatsController.class);

    @Autowired
    private BikeStatsService bikeStatsService;

    @Value("${hdfs.input.path}")
    private String hdfsInputPath;

    // 1. 区域单车分布（饼图）
    @GetMapping("/region-distribution")
    public ResponseEntity<?> getRegionDistribution() {
        log.info("开始处理【区域单车分布】接口请求");
        try {
            Map<String, Integer> data = bikeStatsService.getRegionDistribution();
            // 空数据兜底
            if (data == null || data.isEmpty()) {
                log.warn("【区域单车分布】接口返回空数据，返回兜底值");
                return ResponseEntity.ok(new HashMap<String, Integer>() {{ put("暂无数据", 0); }});
            }
            log.info("【区域单车分布】接口处理完成，返回数据：{}", data);
            return ResponseEntity.ok(data);
        } catch (Exception e) {
            log.error("【区域单车分布】接口执行失败", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("获取区域分布数据失败：" + e.getMessage());
        }
    }

    // 2. 区域平均里程（柱状图）
    @GetMapping("/avg-mileage")
    public ResponseEntity<?> getAverageMileage() {
        log.info("开始处理【区域平均里程】接口请求");
        try {
            Map<String, Double> data = bikeStatsService.getAverageMileageByRegion();
            if (data == null || data.isEmpty()) {
                log.warn("【区域平均里程】接口返回空数据，返回兜底值");
                return ResponseEntity.ok(new HashMap<String, Double>() {{ put("暂无数据", 0.0); }});
            }
            log.info("【区域平均里程】接口处理完成，返回数据：{}", data);
            return ResponseEntity.ok(data);
        } catch (Exception e) {
            log.error("【区域平均里程】接口执行失败", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("获取区域平均里程失败：" + e.getMessage());
        }
    }

    // 3. 品牌数量统计（柱状图）
    @GetMapping("/brand-count")
    public ResponseEntity<?> getBrandCount() {
        log.info("开始处理【品牌数量统计】接口请求");
        try {
            Map<String, Integer> data = bikeStatsService.getBrandCount();
            if (data == null || data.isEmpty()) {
                log.warn("【品牌数量统计】接口返回空数据，返回兜底值");
                return ResponseEntity.ok(new HashMap<String, Integer>() {{ put("暂无数据", 0); }});
            }
            log.info("【品牌数量统计】接口处理完成，返回数据：{}", data);
            return ResponseEntity.ok(data);
        } catch (Exception e) {
            log.error("【品牌数量统计】接口执行失败", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("获取品牌数量统计失败：" + e.getMessage());
        }
    }

    // 4. 投放年份分布
    @GetMapping("/year-distribution")
    public ResponseEntity<?> getYearDistribution() {
        log.info("开始处理【投放年份分布】接口请求");
        try {
            Map<Integer, Integer> data = bikeStatsService.getYearDistribution();
            if (data == null || data.isEmpty()) {
                log.warn("【投放年份分布】接口返回空数据，返回兜底值");
                return ResponseEntity.ok(new HashMap<Integer, Integer>() {{ put(0, 0); }});
            }
            log.info("【投放年份分布】接口处理完成，返回数据：{}", data);
            return ResponseEntity.ok(data);
        } catch (Exception e) {
            log.error("【投放年份分布】接口执行失败", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("获取投放年份分布数据失败：" + e.getMessage());
        }
    }

    // 5. 品牌平均使用时长
    @GetMapping("/avg-usage-time")
    public ResponseEntity<?> getAverageUsageTime() {
        log.info("开始处理【品牌平均使用时长】接口请求");
        try {
            Map<String, Integer> data = bikeStatsService.getAverageUsageTimeByBrand();
            if (data == null || data.isEmpty()) {
                log.warn("【品牌平均使用时长】接口返回空数据，返回兜底值");
                return ResponseEntity.ok(new HashMap<String, Integer>() {{ put("暂无数据", 0); }});
            }
            log.info("【品牌平均使用时长】接口处理完成，返回数据：{}", data);
            return ResponseEntity.ok(data);
        } catch (Exception e) {
            log.error("【品牌平均使用时长】接口执行失败", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("获取品牌平均使用时长失败：" + e.getMessage());
        }
    }

    // 6. 单车状态分布（饼图）
    @GetMapping("/status-distribution")
    public ResponseEntity<?> getStatusDistribution() {
        log.info("开始处理【单车状态分布】接口请求");
        try {
            Map<String, Integer> data = bikeStatsService.getStatusDistribution();
            if (data == null || data.isEmpty()) {
                log.warn("【单车状态分布】接口返回空数据，返回兜底值");
                return ResponseEntity.ok(new HashMap<String, Integer>() {{ put("暂无数据", 0); }});
            }
            log.info("【单车状态分布】接口处理完成，返回数据：{}", data);
            return ResponseEntity.ok(data);
        } catch (Exception e) {
            log.error("【单车状态分布】接口执行失败", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("获取单车状态分布数据失败：" + e.getMessage());
        }
    }

    // 7. 原始数据（用于数据可视化）
    @GetMapping("/raw-data")
    public ResponseEntity<?> getRawData() {
        log.info("开始处理【原始数据】接口请求");
        try {
            List<Map<String, Object>> data = bikeStatsService.getRawData();
            if (data == null || data.isEmpty()) {
                log.warn("【原始数据】接口返回空数据，返回兜底值");
                return ResponseEntity.ok(List.of(new HashMap<String, Object>() {{ put("提示", "暂无原始数据"); }}));
            }
            log.info("【原始数据】接口处理完成，返回数据条数：{}", data.size());
            return ResponseEntity.ok(data);
        } catch (Exception e) {
            log.error("【原始数据】接口执行失败", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("获取原始数据失败：" + e.getMessage());
        }
    }

    // 8. 数据上传功能（新增）
    @PostMapping("/upload-data")
    public ResponseEntity<?> uploadData(@RequestParam("file") MultipartFile file) {
        log.info("开始处理【数据上传】接口请求，文件名：{}", file.getOriginalFilename());
        try {
            // 检查文件类型
            if (!file.getOriginalFilename().endsWith(".csv")) {
                log.warn("【数据上传】文件类型错误，只支持CSV格式");
                return ResponseEntity.badRequest().body("只支持CSV格式文件");
            }

            // 上传文件到HDFS
            String hdfsFilePath = hdfsInputPath + "/" + file.getOriginalFilename();
            HadoopUtil.uploadFile(file.getInputStream(), hdfsFilePath);
            
            log.info("【数据上传】文件上传成功，HDFS路径：{}", hdfsFilePath);
            return ResponseEntity.ok("数据上传成功，系统正在处理中...");
        } catch (IOException e) {
            log.error("【数据上传】文件读取失败", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("文件读取失败：" + e.getMessage());
        } catch (Exception e) {
            log.error("【数据上传】接口执行失败", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("数据上传失败：" + e.getMessage());
        }
    }
}