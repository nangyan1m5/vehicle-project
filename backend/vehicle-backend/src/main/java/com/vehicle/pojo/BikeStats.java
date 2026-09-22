package com.vehicle.pojo;

public class BikeStats {
    private String bikeId;          // 单车ID
    private String brand;           // 品牌
    private String model;           // 型号
    private Integer putYear;        // 投放年份
    private String region;          // 所属区域
    private Double totalMileage;    // 总骑行里程
    private Integer totalUsageTime; // 总使用时长（分钟）
    private String lastUsedTime;    // 最后一次使用时间
    private String status;          // 当前状态（available, maintenance, retired）

    // 无参构造
    public BikeStats() {}

    // 全参构造
    public BikeStats(String bikeId, String brand, String model, Integer putYear, String region, 
                   Double totalMileage, Integer totalUsageTime, String lastUsedTime, String status) {
        this.bikeId = bikeId;
        this.brand = brand;
        this.model = model;
        this.putYear = putYear;
        this.region = region;
        this.totalMileage = totalMileage;
        this.totalUsageTime = totalUsageTime;
        this.lastUsedTime = lastUsedTime;
        this.status = status;
    }

    // Getter & Setter
    public String getBikeId() { return bikeId; }
    public void setBikeId(String bikeId) { this.bikeId = bikeId; }
    
    public String getBrand() { return brand; }
    public void setBrand(String brand) { this.brand = brand; }
    
    public String getModel() { return model; }
    public void setModel(String model) { this.model = model; }
    
    public Integer getPutYear() { return putYear; }
    public void setPutYear(Integer putYear) { this.putYear = putYear; }
    
    public String getRegion() { return region; }
    public void setRegion(String region) { this.region = region; }
    
    public Double getTotalMileage() { return totalMileage; }
    public void setTotalMileage(Double totalMileage) { this.totalMileage = totalMileage; }
    
    public Integer getTotalUsageTime() { return totalUsageTime; }
    public void setTotalUsageTime(Integer totalUsageTime) { this.totalUsageTime = totalUsageTime; }
    
    public String getLastUsedTime() { return lastUsedTime; }
    public void setLastUsedTime(String lastUsedTime) { this.lastUsedTime = lastUsedTime; }
    
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    // 兼容原有字段名称的方法，方便数据迁移
    public void setVehicleId(String vehicleId) { this.bikeId = vehicleId; }
    public String getVehicleId() { return bikeId; }
    
    public void setOwnerRegion(String ownerRegion) { this.region = ownerRegion; }
    public String getOwnerRegion() { return region; }
    
    public void setMileage(Double mileage) { this.totalMileage = mileage; }
    public Double getMileage() { return totalMileage; }
    
    public void setUsageTime(Integer usageTime) { this.totalUsageTime = usageTime; }
    public Integer getUsageTime() { return totalUsageTime; }
    
    public void setYear(Integer year) { this.putYear = year; }
    public Integer getYear() { return putYear; }
    
    public void setProductionYear(String productionYear) {
        try {
            this.putYear = Integer.parseInt(productionYear);
        } catch (NumberFormatException e) {
            this.putYear = 0;
        }
    }
    
    public String getProductionYear() {
        return this.putYear == null ? "0" : this.putYear.toString();
    }
}