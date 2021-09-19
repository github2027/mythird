set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

DROP TABLE IF EXISTS dwt_visitor_topic;
CREATE EXTERNAL TABLE dwt_visitor_topic
(
    `mid_id` STRING COMMENT '设备id',
    `brand` STRING COMMENT '手机品牌',
    `model` STRING COMMENT '手机型号',
    `channel` ARRAY<STRING> COMMENT '渠道',
    `os` ARRAY<STRING> COMMENT '操作系统',
    `area_code` ARRAY<STRING> COMMENT '地区ID',
    `version_code` ARRAY<STRING> COMMENT '应用版本',
    `visit_date_first` STRING  COMMENT '首次访问时间',
    `visit_date_last` STRING  COMMENT '末次访问时间',
    `visit_last_1d_count` BIGINT COMMENT '最近1日访问次数',
    `visit_last_1d_day_count` BIGINT COMMENT '最近1日访问天数',
    `visit_last_7d_count` BIGINT COMMENT '最近7日访问次数',
    `visit_last_7d_day_count` BIGINT COMMENT '最近7日访问天数',
    `visit_last_30d_count` BIGINT COMMENT '最近30日访问次数',
    `visit_last_30d_day_count` BIGINT COMMENT '最近30日访问天数',
    `visit_count` BIGINT COMMENT '累积访问次数',--用户行为日志历史数据无法获取，故该字段实为从数仓搭建日至今的累积值
    `visit_day_count` BIGINT COMMENT '累积访问天数'--用户行为日志历史数据无法获取，故该字段实为从数仓搭建日至今的累积值
) COMMENT '设备主题宽表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall/dwt/dwt_visitor_topic'
TBLPROPERTIES ("parquet.compression"="lzo");

insert overwrite table dwt_visitor_topic partition(dt='2020-06-14')
select
    nvl(1d_ago.mid_id,old.mid_id),
    nvl(1d_ago.brand,old.brand),
    nvl(1d_ago.model,old.model),
    nvl(1d_ago.channel,old.channel),
    nvl(1d_ago.os,old.os),
    nvl(1d_ago.area_code,old.area_code),
    nvl(1d_ago.version_code,old.version_code),
    case when old.mid_id is null and 1d_ago.is_new=1 then '2020-06-14'
         when old.mid_id is null and 1d_ago.is_new=0 then '2020-06-13'--无法获取准确的首次登录日期，给定一个数仓搭建日之前的日期
         else old.visit_date_first end,
    if(1d_ago.mid_id is not null,'2020-06-14',old.visit_date_last),
    nvl(1d_ago.visit_count,0),
    if(1d_ago.mid_id is null,0,1),
    nvl(old.visit_last_7d_count,0)+nvl(1d_ago.visit_count,0)- nvl(7d_ago.visit_count,0),
    nvl(old.visit_last_7d_day_count,0)+if(1d_ago.mid_id is null,0,1)- if(7d_ago.mid_id is null,0,1),
    nvl(old.visit_last_30d_count,0)+nvl(1d_ago.visit_count,0)- nvl(30d_ago.visit_count,0),
    nvl(old.visit_last_30d_day_count,0)+if(1d_ago.mid_id is null,0,1)- if(30d_ago.mid_id is null,0,1),
    nvl(old.visit_count,0)+nvl(1d_ago.visit_count,0),
    nvl(old.visit_day_count,0)+if(1d_ago.mid_id is null,0,1)
from
(
    select
        mid_id,
        brand,
        model,
        channel,
        os,
        area_code,
        version_code,
        visit_date_first,
        visit_date_last,
        visit_last_1d_count,
        visit_last_1d_day_count,
        visit_last_7d_count,
        visit_last_7d_day_count,
        visit_last_30d_count,
        visit_last_30d_day_count,
        visit_count,
        visit_day_count
    from dwt_visitor_topic
    where dt=date_add('2020-06-14',-1)
)old
full outer join
(
    select
        mid_id,
        brand,
        model,
        is_new,
        channel,
        os,
        area_code,
        version_code,
        visit_count
    from dws_visitor_action_daycount
    where dt='2020-06-14'
)1d_ago
on old.mid_id=1d_ago.mid_id
left join
(
    select
        mid_id,
        brand,
        model,
        is_new,
        channel,
        os,
        area_code,
        version_code,
        visit_count
    from dws_visitor_action_daycount
    where dt=date_add('2020-06-14',-7)
)7d_ago
on old.mid_id=7d_ago.mid_id
left join
(
    select
        mid_id,
        brand,
        model,
        is_new,
        channel,
        os,
        area_code,
        version_code,
        visit_count
    from dws_visitor_action_daycount
    where dt=date_add('2020-06-14',-30)
)30d_ago
on old.mid_id=30d_ago.mid_id;

