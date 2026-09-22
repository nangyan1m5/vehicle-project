package com.vehicle.mapper;

import com.vehicle.pojo.BikeStats;
import org.apache.ibatis.annotations.Mapper;
import java.util.List;
import java.util.Map;

@Mapper
public interface BikeStatsMapper {
    // 查询所有单车数据
    List<BikeStats> selectAll();

    // 按投放年份统计单车数量
    List<Map<String, Object>> countByYear();

    // 按品牌统计单车数量
    List<Map<String, Object>> countByBrand();
    
    // 按区域统计单车数量
    List<Map<String, Object>> countByRegion();
    
    // 按状态统计单车数量
    List<Map<String, Object>> countByStatus();
    
    // 按区域统计平均骑行里程
    List<Map<String, Object>> avgMileageByRegion();
    
    // 按品牌统计平均使用时长
    List<Map<String, Object>> avgUsageTimeByBrand();
}