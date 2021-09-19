--DWD 明细数据层
select get_json_object('[{"name":"大郎","sex":"男","age":"25"},{"name":"西门庆","sex":"男","age":"47"}]', '$[0]');
select get_json_object('[{"name":"大郎","sex":"男","age":"25"},{"name":"西门庆","sex":"男","age":"47"}]', '$[0].age');

--启动日志 每行数据对应一个启动记录
SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

--动作信息表中每行数据对应用户的一个动作记录，一个动作记录应该包含公共信息、页面信息以及动作信息。
--动作信息来来自于普通页面日志中的动作数组。而DWD层的动作信息表中一行数据表示一个动作，所以需要将ODS层的页面使用函数炸开（一进多出UDTF）
-- explode需要应用于数组或是Map集合，String类型无法使用，所以需要自定义UDTF

create function explode_json_array
    as 'com.atguigu.gmall.hive.udtf.ExplodeJSONArray'
    using jar 'hdfs://hadoop102:8020/user/hive/jars/Hive-Udtf-1.0-SNAPSHOT.jar';

show functions like "*json*";

select explode_json_array(1);

drop function explode_json_array;

select *
from ods_log lateral view explode_json_array(get_json_object(line, '$.actions')) tmp as action
where dt = '2020-06-14'
  and get_json_object(line, '$.actions') is not null;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table dwd_action_log partition (dt = '2020-06-14')
select get_json_object(line, '$.common.ar'),
       get_json_object(line, '$.common.ba'),
       get_json_object(line, '$.common.ch'),
       get_json_object(line, '$.common.is_new'),
       get_json_object(line, '$.common.md'),
       get_json_object(line, '$.common.mid'),
       get_json_object(line, '$.common.os'),
       get_json_object(line, '$.common.uid'),
       get_json_object(line, '$.common.vc'),
       get_json_object(line, '$.page.during_time'),
       get_json_object(line, '$.page.item'),
       get_json_object(line, '$.page.item_type'),
       get_json_object(line, '$.page.last_page_id'),
       get_json_object(line, '$.page.page_id'),
       get_json_object(line, '$.page.source_type'),
       get_json_object(action, '$.action_id'),
       get_json_object(action, '$.item'),
       get_json_object(action, '$.item_type'),
       get_json_object(action, '$.ts')
from ods_log lateral view explode_json_array(get_json_object(line, '$.actions')) tmp as action
where dt = '2020-06-14'
  and get_json_object(line, '$.actions') is not null;

select *
from dwd_action_log
where dt = '2020-06-14';

CREATE EXTERNAL TABLE dwd_display_log
(
    `area_code`      STRING COMMENT '地区编码',
    `brand`          STRING COMMENT '手机品牌',
    `channel`        STRING COMMENT '渠道',
    `is_new`         STRING COMMENT '是否首次启动',
    `model`          STRING COMMENT '手机型号',
    `mid_id`         STRING COMMENT '设备id',
    `os`             STRING COMMENT '操作系统',
    `user_id`        STRING COMMENT '会员id',
    `version_code`   STRING COMMENT 'app版本号',
    `during_time`    BIGINT COMMENT '页面持续时间',
    `page_item`      STRING COMMENT '目标id ',
    `page_item_type` STRING COMMENT '目标类型',
    `last_page_id`   STRING COMMENT '上页类型',
    `page_id`        STRING COMMENT '页面ID ',
    `source_type`    STRING COMMENT '来源类型',
    `ts`             BIGINT COMMENT '页面跳入时间(曝光时间)',
    `display_type`   STRING COMMENT '曝光类型',
    `item`           STRING COMMENT '曝光对象id ',
    `item_type`      STRING COMMENT '曝光对象类型',
    `order`          BIGINT COMMENT '曝光顺序',
    `pos_id`         BIGINT COMMENT '曝光位置'
) COMMENT '曝光日志表'
    PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall/dwd/dwd_display_log'
    TBLPROPERTIES ('parquet.compression' = 'lzo');

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table dwd_display_log partition (dt = '2020-06-14')
select get_json_object(line, '$.common.ar'),
       get_json_object(line, '$.common.ba'),
       get_json_object(line, '$.common.ch'),
       get_json_object(line, '$.common.is_new'),
       get_json_object(line, '$.common.md'),
       get_json_object(line, '$.common.mid'),
       get_json_object(line, '$.common.os'),
       get_json_object(line, '$.common.uid'),
       get_json_object(line, '$.common.vc'),
       get_json_object(line, '$.page.during_time'),
       get_json_object(line, '$.page.item'),
       get_json_object(line, '$.page.item_type'),
       get_json_object(line, '$.page.last_page_id'),
       get_json_object(line, '$.page.page_id'),
       get_json_object(line, '$.page.source_type'),
       get_json_object(line, '$.ts'),
       get_json_object(display, '$.display_type'),
       get_json_object(display, '$.item'),
       get_json_object(display, '$.item_type'),
       get_json_object(display, '$.order'),
       get_json_object(display, '$.pos_id')
from ods_log lateral view explode_json_array(get_json_object(line, '$.displays')) tmp as display
where dt = '2020-06-14'
  and get_json_object(line, '$.displays') is not null;

select *
from dwd_display_log
where dt = '2020-06-14';

CREATE EXTERNAL TABLE dwd_error_log
(
    `area_code`       STRING COMMENT '地区编码',
    `brand`           STRING COMMENT '手机品牌',
    `channel`         STRING COMMENT '渠道',
    `is_new`          STRING COMMENT '是否首次启动',
    `model`           STRING COMMENT '手机型号',
    `mid_id`          STRING COMMENT '设备id',
    `os`              STRING COMMENT '操作系统',
    `user_id`         STRING COMMENT '会员id',
    `version_code`    STRING COMMENT 'app版本号',
    `page_item`       STRING COMMENT '目标id ',
    `page_item_type`  STRING COMMENT '目标类型',
    `last_page_id`    STRING COMMENT '上页类型',
    `page_id`         STRING COMMENT '页面ID ',
    `source_type`     STRING COMMENT '来源类型',
    `entry`           STRING COMMENT 'icon手机图标 notice通知 install安装后启动',
    `loading_time`    STRING COMMENT '启动加载时间',
    `open_ad_id`      STRING COMMENT '广告页ID ',
    `open_ad_ms`      STRING COMMENT '广告总共播放时间',
    `open_ad_skip_ms` STRING COMMENT '用户跳过广告时点',
    `actions`         STRING COMMENT '动作数组',
    `displays`        STRING COMMENT '曝光数组',
    `ts`              STRING COMMENT '页面跳入时间',
    `error_code`      STRING COMMENT '错误码',
    `msg`             STRING COMMENT '错误信息'
) COMMENT '错误日志表'
    PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall/dwd/dwd_error_log'
    TBLPROPERTIES ('parquet.compression' = 'lzo');

insert overwrite table dwd_error_log partition (dt = '2020-06-14')
select get_json_object(line, '$.common.ar'),
       get_json_object(line, '$.common.ba'),
       get_json_object(line, '$.common.ch'),
       get_json_object(line, '$.common.is_new'),
       get_json_object(line, '$.common.md'),
       get_json_object(line, '$.common.mid'),
       get_json_object(line, '$.common.os'),
       get_json_object(line, '$.common.uid'),
       get_json_object(line, '$.common.vc'),
       get_json_object(line, '$.page.item'),
       get_json_object(line, '$.page.item_type'),
       get_json_object(line, '$.page.last_page_id'),
       get_json_object(line, '$.page.page_id'),
       get_json_object(line, '$.page.source_type'),
       get_json_object(line, '$.start.entry'),
       get_json_object(line, '$.start.loading_time'),
       get_json_object(line, '$.start.open_ad_id'),
       get_json_object(line, '$.start.open_ad_ms'),
       get_json_object(line, '$.start.open_ad_skip_ms'),
       get_json_object(line, '$.actions'),
       get_json_object(line, '$.displays'),
       get_json_object(line, '$.ts'),
       get_json_object(line, '$.err.error_code'),
       get_json_object(line, '$.err.msg')
from ods_log
where dt = '2020-06-14'
  and get_json_object(line, '$.err') is not null;

select *
from dwd_error_log
where dt = '2020-06-14'
limit 2;

CREATE EXTERNAL TABLE dwd_comment_info
(
    `id`          STRING COMMENT '编号',
    `user_id`     STRING COMMENT '用户ID',
    `sku_id`      STRING COMMENT '商品sku',
    `spu_id`      STRING COMMENT '商品spu',
    `order_id`    STRING COMMENT '订单ID',
    `appraise`    STRING COMMENT '评价(好评、中评、差评、默认评价)',
    `create_time` STRING COMMENT '评价时间'
) COMMENT '评价事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall/dwd/dwd_comment_info/'
    TBLPROPERTIES ("parquet.compression" = "lzo");

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_comment_info partition (dt)
select id,
       user_id,
       sku_id,
       spu_id,
       order_id,
       appraise,
       create_time,
       date_format(create_time, 'yyyy-MM-dd')
from ods_comment_info
where dt = '2020-06-14';

CREATE EXTERNAL TABLE dwd_order_detail
(
    `id`                    STRING COMMENT '订单编号',
    `order_id`              STRING COMMENT '订单号',
    `user_id`               STRING COMMENT '用户id',
    `sku_id`                STRING COMMENT 'sku商品id',
    `province_id`           STRING COMMENT '省份ID',
    `activity_id`           STRING COMMENT '活动ID',
    `activity_rule_id`      STRING COMMENT '活动规则ID',
    `coupon_id`             STRING COMMENT '优惠券ID',
    `create_time`           STRING COMMENT '创建时间',
    `source_type`           STRING COMMENT '来源类型',
    `source_id`             STRING COMMENT '来源编号',
    `sku_num`               BIGINT COMMENT '商品数量',
    `original_amount`       DECIMAL(16, 2) COMMENT '原始价格',
    `split_activity_amount` DECIMAL(16, 2) COMMENT '活动优惠分摊',
    `split_coupon_amount`   DECIMAL(16, 2) COMMENT '优惠券优惠分摊',
    `split_final_amount`    DECIMAL(16, 2) COMMENT '最终价格分摊'
) COMMENT '订单明细事实表表'
    PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall/dwd/dwd_order_detail/'
    TBLPROPERTIES ("parquet.compression" = "lzo");

insert overwrite table dwd_order_detail partition (dt)
select od.id,
       od.order_id,
       oi.user_id,
       od.sku_id,
       oi.province_id,
       oda.activity_id,
       oda.activity_rule_id,
       odc.coupon_id,
       od.create_time,
       od.source_type,
       od.source_id,
       od.sku_num,
       od.order_price * od.sku_num,
       od.split_activity_amount,
       od.split_coupon_amount,
       od.split_final_amount,
       date_format(create_time, 'yyyy-MM-dd')
from (
         select *
         from ods_order_detail
         where dt = '2020-06-14'
     ) od
         left join
     (
         select id,
                user_id,
                province_id
         from ods_order_info
         where dt = '2020-06-14'
     ) oi
     on od.order_id = oi.id
         left join
     (
         select order_detail_id,
                activity_id,
                activity_rule_id
         from ods_order_detail_activity
         where dt = '2020-06-14'
     ) oda
     on od.id = oda.order_detail_id
         left join
     (
         select order_detail_id,
                coupon_id
         from ods_order_detail_coupon
         where dt = '2020-06-14'
     ) odc
     on od.id = odc.order_detail_id;

DROP TABLE IF EXISTS dwd_order_refund_info;
CREATE EXTERNAL TABLE dwd_order_refund_info
(
    `id`                 STRING COMMENT '编号',
    `user_id`            STRING COMMENT '用户ID',
    `order_id`           STRING COMMENT '订单ID',
    `sku_id`             STRING COMMENT '商品ID',
    `province_id`        STRING COMMENT '地区ID',
    `refund_type`        STRING COMMENT '退单类型',
    `refund_num`         BIGINT COMMENT '退单件数',
    `refund_amount`      DECIMAL(16, 2) COMMENT '退单金额',
    `refund_reason_type` STRING COMMENT '退单原因类型',
    `create_time`        STRING COMMENT '退单时间'
) COMMENT '退单事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall/dwd/dwd_order_refund_info/'
    TBLPROPERTIES ("parquet.compression" = "lzo");

insert overwrite table dwd_order_refund_info partition (dt)
select ri.id,
       ri.user_id,
       ri.order_id,
       ri.sku_id,
       oi.province_id,
       ri.refund_type,
       ri.refund_num,
       ri.refund_amount,
       ri.refund_reason_type,
       ri.create_time,
       date_format(ri.create_time, 'yyyy-MM-dd')
from (
         select *
         from ods_order_refund_info
         where dt = '2020-06-14'
     ) ri
         left join
     (
         select id, province_id
         from ods_order_info
         where dt = '2020-06-14'
     ) oi
     on ri.order_id = oi.id;

show databases

DROP TABLE IF EXISTS dwd_favor_info;
CREATE EXTERNAL TABLE dwd_favor_info
(
    `id`          STRING COMMENT '编号',
    `user_id`     STRING COMMENT '用户id',
    `sku_id`      STRING COMMENT 'skuid',
    `spu_id`      STRING COMMENT 'spuid',
    `is_cancel`   STRING COMMENT '是否取消',
    `create_time` STRING COMMENT '收藏时间',
    `cancel_time` STRING COMMENT '取消时间'
) COMMENT '收藏事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall/dwd/dwd_favor_info/'
    TBLPROPERTIES ("parquet.compression" = "lzo");

insert into table dwd_favor_info partition dt='2020-06-14'
select
       id,
       user_id,
       sku_id,
       spu_id,
       is_cancel,
       create_time,
       cancel_time
from ods_favor_info
where dt ='2020-06-14'

