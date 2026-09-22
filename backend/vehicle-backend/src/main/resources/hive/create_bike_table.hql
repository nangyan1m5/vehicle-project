-- 创建共享单车统计分析表（与vehicle_db同级）
drop table if exists bike_stats;
create table bike_stats(
    bike_id string comment '单车ID',
    brand string comment '品牌',
    production_year int comment '生产年份',
    region string comment '所属区域',
    mileage double comment '行驶里程',
    usage_time int comment '使用时长（分钟）',
    status string comment '单车状态（正常、维修中、报废）'
) 
row format delimited 
fields terminated by ',' 
stored as textfile 
location '/user/hive/warehouse/bike_stats' 
TBLPROPERTIES ('serialization.null.format'='', 'skip.header.line.count'='1');

-- 创建外部表用于数据导入
drop table if exists bike_stats_external;
create external table bike_stats_external(
    bike_id string,
    brand string,
    production_year int,
    region string,
    mileage double,
    usage_time int,
    status string
) 
row format delimited 
fields terminated by ',' 
stored as textfile 
location '/user/hive/warehouse/vehicle_db/external/bike_data' 
TBLPROPERTIES ('serialization.null.format'='', 'skip.header.line.count'='1');

-- 从外部表导入数据到内部表
truncate table bike_stats;
insert into table bike_stats 
select * from bike_stats_external;

-- 创建分区表（按年份）
drop table if exists bike_stats_partitioned;
create table bike_stats_partitioned(
    bike_id string,
    brand string,
    region string,
    mileage double,
    usage_time int,
    status string
) 
partitioned by (production_year int)
row format delimited 
fields terminated by ',' 
stored as textfile 
location '/user/hive/warehouse/bike_stats_partitioned' 
TBLPROPERTIES ('serialization.null.format'='', 'skip.header.line.count'='1');

-- 插入数据到分区表
truncate table bike_stats_partitioned;
insert into table bike_stats_partitioned partition(production_year)
select bike_id, brand, region, mileage, usage_time, status, production_year from bike_stats;
