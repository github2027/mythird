set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

---------------------------------------dim_sku_info----------------------------------
CREATE EXTERNAL TABLE dim_sku_info (
    `id` STRING COMMENT '商品id',
    `price` DECIMAL(16,2) COMMENT '商品价格',
    `sku_name` STRING COMMENT '商品名称',
    `sku_desc` STRING COMMENT '商品描述',
    `weight` DECIMAL(16,2) COMMENT '重量',
    `is_sale` BOOLEAN COMMENT '是否在售',
    `spu_id` STRING COMMENT 'spu编号',
    `spu_name` STRING COMMENT 'spu名称',
    `category3_id` STRING COMMENT '三级分类id',
    `category3_name` STRING COMMENT '三级分类名称',
    `category2_id` STRING COMMENT '二级分类id',
    `category2_name` STRING COMMENT '二级分类名称',
    `category1_id` STRING COMMENT '一级分类id',
    `category1_name` STRING COMMENT '一级分类名称',
    `tm_id` STRING COMMENT '品牌id',
    `tm_name` STRING COMMENT '品牌名称',
    `sku_attr_values` ARRAY<STRUCT<attr_id:STRING,value_id:STRING,attr_name:STRING,value_name:STRING>> COMMENT '平台属性',
    `sku_sale_attr_values` ARRAY<STRUCT<sale_attr_id:STRING,sale_attr_value_id:STRING,sale_attr_name:STRING,sale_attr_value_name:STRING>> COMMENT '销售属性',
    `create_time` STRING COMMENT '创建时间'
) COMMENT '商品维度表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall/dim/dim_sku_info/'
TBLPROPERTIES ("parquet.compression"="lzo");

with
sku as
(
    select
        id,
        price,
        sku_name,
        sku_desc,
        weight,
        is_sale,
        spu_id,
        category3_id,
        tm_id,
        create_time
    from ods_sku_info
    where dt='2020-06-14'
),
spu as
(
    select
        id,
        spu_name
    from ods_spu_info
    where dt='2020-06-14'
),
c3 as
(
    select
        id,
        name,
        category2_id
    from ods_base_category3
    where dt='2020-06-14'
),
c2 as
(
    select
        id,
        name,
        category1_id
    from ods_base_category2
    where dt='2020-06-14'
),
c1 as
(
    select
        id,
        name
    from ods_base_category1
    where dt='2020-06-14'
),
tm as
(
    select
        id,
        tm_name
    from ods_base_trademark
    where dt='2020-06-14'
),
attr as
(
    select
        sku_id,
        collect_set(named_struct('attr_id',attr_id,'value_id',value_id,'attr_name',attr_name,'value_name',value_name)) attrs
    from ods_sku_attr_value
    where dt='2020-06-14'
    group by sku_id
),
sale_attr as
(
    select
        sku_id,
        collect_set(named_struct('sale_attr_id',sale_attr_id,'sale_attr_value_id',sale_attr_value_id,'sale_attr_name',sale_attr_name,'sale_attr_value_name',sale_attr_value_name)) sale_attrs
    from ods_sku_sale_attr_value
    where dt='2020-06-14'
    group by sku_id
)
insert overwrite table dim_sku_info partition(dt='2020-06-14')
select
    sku.id,
    sku.price,
    sku.sku_name,
    sku.sku_desc,
    sku.weight,
    sku.is_sale,
    sku.spu_id,
    spu.spu_name,
    sku.category3_id,
    c3.name,
    c3.category2_id,
    c2.name,
    c2.category1_id,
    c1.name,
    sku.tm_id,
    tm.tm_name,
    attr.attrs,
    sale_attr.sale_attrs,
    sku.create_time
from sku
left join spu on sku.spu_id=spu.id
left join c3 on sku.category3_id=c3.id
left join c2 on c3.category2_id=c2.id
left join c1 on c2.category1_id=c1.id
left join tm on sku.tm_id=tm.id
left join attr on sku.id=attr.sku_id
left join sale_attr on sku.id=sale_attr.sku_id;

---------------------------------------dim_coupon_info----------------------------------
DROP TABLE IF EXISTS dim_coupon_info;
CREATE EXTERNAL TABLE dim_coupon_info(
    `id` STRING COMMENT '购物券编号',
    `coupon_name` STRING COMMENT '购物券名称',
    `coupon_type` STRING COMMENT '购物券类型 1 现金券 2 折扣券 3 满减券 4 满件打折券',
    `condition_amount` DECIMAL(16,2) COMMENT '满额数',
    `condition_num` BIGINT COMMENT '满件数',
    `activity_id` STRING COMMENT '活动编号',
    `benefit_amount` DECIMAL(16,2) COMMENT '减金额',
    `benefit_discount` DECIMAL(16,2) COMMENT '折扣',
    `create_time` STRING COMMENT '创建时间',
    `range_type` STRING COMMENT '范围类型 1、商品 2、品类 3、品牌',
    `limit_num` BIGINT COMMENT '最多领取次数',
    `taken_count` BIGINT COMMENT '已领取次数',
    `start_time` STRING COMMENT '可以领取的开始日期',
    `end_time` STRING COMMENT '可以领取的结束日期',
    `operate_time` STRING COMMENT '修改时间',
    `expire_time` STRING COMMENT '过期时间'
) COMMENT '优惠券维度表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall/dim/dim_coupon_info/'
TBLPROPERTIES ("parquet.compression"="lzo");

insert overwrite table dim_coupon_info partition(dt='2020-06-14')
select
    id,
    coupon_name,
    coupon_type,
    condition_amount,
    condition_num,
    activity_id,
    benefit_amount,
    benefit_discount,
    create_time,
    range_type,
    limit_num,
    taken_count,
    start_time,
    end_time,
    operate_time,
    expire_time
from ods_coupon_info
where dt='2020-06-14';

---------------------------------------dim_activity_rule_info----------------------------------
DROP TABLE IF EXISTS dim_activity_rule_info;
CREATE EXTERNAL TABLE dim_activity_rule_info(
    `activity_rule_id` STRING COMMENT '活动规则ID',
    `activity_id` STRING COMMENT '活动ID',
    `activity_name` STRING  COMMENT '活动名称',
    `activity_type` STRING  COMMENT '活动类型',
    `start_time` STRING  COMMENT '开始时间',
    `end_time` STRING  COMMENT '结束时间',
    `create_time` STRING  COMMENT '创建时间',
    `condition_amount` DECIMAL(16,2) COMMENT '满减金额',
    `condition_num` BIGINT COMMENT '满减件数',
    `benefit_amount` DECIMAL(16,2) COMMENT '优惠金额',
    `benefit_discount` DECIMAL(16,2) COMMENT '优惠折扣',
    `benefit_level` STRING COMMENT '优惠级别'
) COMMENT '活动信息表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall/dim/dim_activity_rule_info/'
TBLPROPERTIES ("parquet.compression"="lzo");

insert overwrite table dim_activity_rule_info partition(dt='2020-06-14')
select
    ar.id,
    ar.activity_id,
    ai.activity_name,
    ar.activity_type,
    ai.start_time,
    ai.end_time,
    ai.create_time,
    ar.condition_amount,
    ar.condition_num,
    ar.benefit_amount,
    ar.benefit_discount,
    ar.benefit_level
from
(
    select
        id,
        activity_id,
        activity_type,
        condition_amount,
        condition_num,
        benefit_amount,
        benefit_discount,
        benefit_level
    from ods_activity_rule
    where dt='2020-06-14'
)ar
left join
(
    select
        id,
        activity_name,
        start_time,
        end_time,
        create_time
    from ods_activity_info
    where dt='2020-06-14'
)ai
on ar.activity_id=ai.id;

---------------------------------------dim_base_province----------------------------------
DROP TABLE IF EXISTS dim_base_province;
CREATE EXTERNAL TABLE dim_base_province (
    `id` STRING COMMENT 'id',
    `province_name` STRING COMMENT '省市名称',
    `area_code` STRING COMMENT '地区编码',
    `iso_code` STRING COMMENT 'ISO-3166编码，供可视化使用',
    `iso_3166_2` STRING COMMENT 'IOS-3166-2编码，供可视化使用',
    `region_id` STRING COMMENT '地区id',
    `region_name` STRING COMMENT '地区名称'
) COMMENT '地区维度表'
STORED AS PARQUET
LOCATION '/warehouse/gmall/dim/dim_base_province/'
TBLPROPERTIES ("parquet.compression"="lzo");

insert overwrite table dim_base_province
select
    bp.id,
    bp.name,
    bp.area_code,
    bp.iso_code,
    bp.iso_3166_2,
    bp.region_id,
    br.region_name
from ods_base_province bp
join ods_base_region br on bp.region_id = br.id;

---------------------------------------dim_date_info----------------------------------
DROP TABLE IF EXISTS dim_date_info;
CREATE EXTERNAL TABLE dim_date_info(
    `date_id` STRING COMMENT '日',
    `week_id` STRING COMMENT '周ID',
    `week_day` STRING COMMENT '周几',
    `day` STRING COMMENT '每月的第几天',
    `month` STRING COMMENT '第几月',
    `quarter` STRING COMMENT '第几季度',
    `year` STRING COMMENT '年',
    `is_workday` STRING COMMENT '是否是工作日',
    `holiday_id` STRING COMMENT '节假日'
) COMMENT '时间维度表'
STORED AS PARQUET
LOCATION '/warehouse/gmall/dim/dim_date_info/'
TBLPROPERTIES ("parquet.compression"="lzo");

insert overwrite table dim_date_info select * from tmp_dim_date_info;

---------------------------------------dim_user_info----------------------------------
DROP TABLE IF EXISTS dim_user_info;
CREATE EXTERNAL TABLE dim_user_info(
    `id` STRING COMMENT '用户id',
    `login_name` STRING COMMENT '用户名称',
    `nick_name` STRING COMMENT '用户昵称',
    `name` STRING COMMENT '用户姓名',
    `phone_num` STRING COMMENT '手机号码',
    `email` STRING COMMENT '邮箱',
    `user_level` STRING COMMENT '用户等级',
    `birthday` STRING COMMENT '生日',
    `gender` STRING COMMENT '性别',
    `create_time` STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '操作时间',
    `start_date` STRING COMMENT '开始日期',
    `end_date` STRING COMMENT '结束日期'
) COMMENT '用户表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall/dim/dim_user_info/'
TBLPROPERTIES ("parquet.compression"="lzo");

insert overwrite table dim_user_info partition(dt='9999-99-99')
select
    id,
    login_name,
    nick_name,
    md5(name),
    md5(phone_num),
    md5(email),
    user_level,
    birthday,
    gender,
    create_time,
    operate_time,
    '2020-06-14',
    '9999-99-99'
from ods_user_info
where dt='2020-06-14';

with
tmp as
(
    select
        nvl(ods_ui.id,dim_ui.id) new_id,
        nvl(ods_ui.login_name,dim_ui.login_name) new_login_name,
        nvl(ods_ui.nick_name,dim_ui.nick_name) new_nick_name,
        nvl(ods_ui.name,dim_ui.name) new_name,
        nvl(ods_ui.phone_num,dim_ui.phone_num) new_phone_num,
        nvl(ods_ui.email,dim_ui.email) new_email,
        nvl(ods_ui.user_level,dim_ui.user_level) new_user_level,
        nvl(ods_ui.birthday,dim_ui.birthday) new_birthday,
        nvl(ods_ui.gender,dim_ui.gender) new_gender,
        nvl(ods_ui.create_time,dim_ui.create_time) new_create_time,
        nvl(ods_ui.operate_time,dim_ui.operate_time) new_operate_time,
        nvl(ods_ui.start_date,dim_ui.start_date) new_start_date,
        nvl(ods_ui.end_date,dim_ui.end_date) new_end_date,
        if(ods_ui.id is not null and dim_ui.id is not null,dim_ui.id,null) old_id,
        if(ods_ui.id is not null and dim_ui.id is not null,dim_ui.login_name,null) old_login_name,
        if(ods_ui.id is not null and dim_ui.id is not null,dim_ui.nick_name,null) old_nick_name,
        if(ods_ui.id is not null and dim_ui.id is not null,dim_ui.name,null) old_name,
        if(ods_ui.id is not null and dim_ui.id is not null,dim_ui.phone_num,null) old_phone_num,
        if(ods_ui.id is not null and dim_ui.id is not null,dim_ui.email,null) old_email,
        if(ods_ui.id is not null and dim_ui.id is not null,dim_ui.user_level,null) old_user_level,
        if(ods_ui.id is not null and dim_ui.id is not null,dim_ui.birthday,null) old_birthday,
        if(ods_ui.id is not null and dim_ui.id is not null,dim_ui.gender,null) old_gender,
        if(ods_ui.id is not null and dim_ui.id is not null,dim_ui.create_time,null) old_create_time,
        if(ods_ui.id is not null and dim_ui.id is not null,dim_ui.operate_time,null) old_operate_time,
        if(ods_ui.id is not null and dim_ui.id is not null,dim_ui.start_date,null) old_start_date,
        cast(if(ods_ui.id is not null and dim_ui.id is not null,date_add('2020-06-15',-1),null) as string) old_end_date
    from
    (
        select
            id,
            login_name,
            nick_name,
            name,
            phone_num,
            email,
            user_level,
            birthday,
            gender,
            create_time,
            operate_time,
            start_date,
            end_date
        from dim_user_info
        where dt='9999-99-99'
    ) dim_ui
    full join
    (
        select
            id,
            login_name,
            nick_name,
            md5(name) name,
            md5(phone_num) phone_num,
            md5(email) email,
            user_level,
            birthday,
            gender,
            create_time,
            operate_time,
            '2020-06-15' start_date,
            '9999-99-99' end_date
        from ods_user_info
        where dt='2020-06-15'
    ) ods_ui
    on dim_ui.id = ods_ui.id
)
insert overwrite table dim_user_info partition(dt)
select
    new_id,
    new_login_name,
    new_nick_name,
    new_name,
    new_phone_num,
    new_email,
    new_user_level,
    new_birthday,
    new_gender,
    new_create_time,
    new_operate_time,
    new_start_date,
    new_end_date,
    new_end_date dt
from tmp
union all
select
    old_id,
    old_login_name,
    old_nick_name,
    old_name,
    old_phone_num,
    old_email,
    old_user_level,
    old_birthday,
    old_gender,
    old_create_time,
    old_operate_time,
    old_start_date,
    old_end_date,
    old_end_date dt
from tmp
where old_id is not null;

---------------------------------------dwd_start_log----------------------------------
DROP TABLE IF EXISTS dwd_start_log;
CREATE EXTERNAL TABLE dwd_start_log(
    `area_code` STRING COMMENT '地区编码',
    `brand` STRING COMMENT '手机品牌',
    `channel` STRING COMMENT '渠道',
    `is_new` STRING COMMENT '是否首次启动',
    `model` STRING COMMENT '手机型号',
    `mid_id` STRING COMMENT '设备id',
    `os` STRING COMMENT '操作系统',
    `user_id` STRING COMMENT '会员id',
    `version_code` STRING COMMENT 'app版本号',
    `entry` STRING COMMENT 'icon手机图标 notice通知 install安装后启动',
    `loading_time` BIGINT COMMENT '启动加载时间',
    `open_ad_id` STRING COMMENT '广告页ID ',
    `open_ad_ms` BIGINT COMMENT '广告总共播放时间',
    `open_ad_skip_ms` BIGINT COMMENT '用户跳过广告时点',
    `ts` BIGINT COMMENT '启动时间'
) COMMENT '启动日志表'
PARTITIONED BY (`dt` STRING) -- 按照时间创建分区
STORED AS PARQUET -- 采用parquet列式存储
LOCATION '/warehouse/gmall/dwd/dwd_start_log'--指定在HDFS上存储位置
TBLPROPERTIES('parquet.compression'='lzo') -- 采用LZO压缩
;

insert overwrite table dwd_start_log partition(dt='2020-06-14')
select
    get_json_object(line,'$.common.ar'),
    get_json_object(line,'$.common.ba'),
    get_json_object(line,'$.common.ch'),
    get_json_object(line,'$.common.is_new'),
    get_json_object(line,'$.common.md'),
    get_json_object(line,'$.common.mid'),
    get_json_object(line,'$.common.os'),
    get_json_object(line,'$.common.uid'),
    get_json_object(line,'$.common.vc'),
    get_json_object(line,'$.start.entry'),
    get_json_object(line,'$.start.loading_time'),
    get_json_object(line,'$.start.open_ad_id'),
    get_json_object(line,'$.start.open_ad_ms'),
    get_json_object(line,'$.start.open_ad_skip_ms'),
    get_json_object(line,'$.ts')
from ods_log
where dt='2020-06-14'
and get_json_object(line,'$.start') is not null;

---------------------------------------dwd_page_log----------------------------------
DROP TABLE IF EXISTS dwd_page_log;
CREATE EXTERNAL TABLE dwd_page_log(
    `area_code` STRING COMMENT '地区编码',
    `brand` STRING COMMENT '手机品牌',
    `channel` STRING COMMENT '渠道',
    `is_new` STRING COMMENT '是否首次启动',
    `model` STRING COMMENT '手机型号',
    `mid_id` STRING COMMENT '设备id',
    `os` STRING COMMENT '操作系统',
    `user_id` STRING COMMENT '会员id',
    `version_code` STRING COMMENT 'app版本号',
    `during_time` BIGINT COMMENT '持续时间毫秒',
    `page_item` STRING COMMENT '目标id ',
    `page_item_type` STRING COMMENT '目标类型',
    `last_page_id` STRING COMMENT '上页类型',
    `page_id` STRING COMMENT '页面ID ',
    `source_type` STRING COMMENT '来源类型',
    `ts` bigint COMMENT '页面跳入时间'
) COMMENT '页面日志表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall/dwd/dwd_page_log'
TBLPROPERTIES('parquet.compression'='lzo');

insert overwrite table dwd_page_log partition(dt='2020-06-14')
select
    get_json_object(line,'$.common.ar'),
    get_json_object(line,'$.common.ba'),
    get_json_object(line,'$.common.ch'),
    get_json_object(line,'$.common.is_new'),
    get_json_object(line,'$.common.md'),
    get_json_object(line,'$.common.mid'),
    get_json_object(line,'$.common.os'),
    get_json_object(line,'$.common.uid'),
    get_json_object(line,'$.common.vc'),
    get_json_object(line,'$.page.during_time'),
    get_json_object(line,'$.page.item'),
    get_json_object(line,'$.page.item_type'),
    get_json_object(line,'$.page.last_page_id'),
    get_json_object(line,'$.page.page_id'),
    get_json_object(line,'$.page.source_type'),
    get_json_object(line,'$.ts')
from ods_log
where dt='2020-06-14'
and get_json_object(line,'$.page') is not null;

---------------------------------------dwd_action_log----------------------------------
DROP TABLE IF EXISTS dwd_action_log;
CREATE EXTERNAL TABLE dwd_action_log(
    `area_code` STRING COMMENT '地区编码',
    `brand` STRING COMMENT '手机品牌',
    `channel` STRING COMMENT '渠道',
    `is_new` STRING COMMENT '是否首次启动',
    `model` STRING COMMENT '手机型号',
    `mid_id` STRING COMMENT '设备id',
    `os` STRING COMMENT '操作系统',
    `user_id` STRING COMMENT '会员id',
    `version_code` STRING COMMENT 'app版本号',
    `during_time` BIGINT COMMENT '持续时间毫秒',
    `page_item` STRING COMMENT '目标id ',
    `page_item_type` STRING COMMENT '目标类型',
    `last_page_id` STRING COMMENT '上页类型',
    `page_id` STRING COMMENT '页面id ',
    `source_type` STRING COMMENT '来源类型',
    `action_id` STRING COMMENT '动作id',
    `item` STRING COMMENT '目标id ',
    `item_type` STRING COMMENT '目标类型',
    `ts` BIGINT COMMENT '动作发生时间'
) COMMENT '动作日志表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall/dwd/dwd_action_log'
TBLPROPERTIES('parquet.compression'='lzo');

---------------------------------------dwd_action_log----------------------------------
CREATE EXTERNAL TABLE dwd_comment_info(
    `id` STRING COMMENT '编号',
    `user_id` STRING COMMENT '用户ID',
    `sku_id` STRING COMMENT '商品sku',
    `spu_id` STRING COMMENT '商品spu',
    `order_id` STRING COMMENT '订单ID',
    `appraise` STRING COMMENT '评价(好评、中评、差评、默认评价)',
    `create_time` STRING COMMENT '评价时间'
) COMMENT '评价事实表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall/dwd/dwd_comment_info/'
TBLPROPERTIES ("parquet.compression"="lzo");

insert overwrite table dwd_comment_info partition (dt)
select
    id,
    user_id,
    sku_id,
    spu_id,
    order_id,
    appraise,
    create_time,
    date_format(create_time,'yyyy-MM-dd')
from ods_comment_info
where dt='2020-06-14';

---------------------------------------dwd_order_detail----------------------------------
CREATE EXTERNAL TABLE dwd_order_detail (
    `id` STRING COMMENT '订单编号',
    `order_id` STRING COMMENT '订单号',
    `user_id` STRING COMMENT '用户id',
    `sku_id` STRING COMMENT 'sku商品id',
    `province_id` STRING COMMENT '省份ID',
    `activity_id` STRING COMMENT '活动ID',
    `activity_rule_id` STRING COMMENT '活动规则ID',
    `coupon_id` STRING COMMENT '优惠券ID',
    `create_time` STRING COMMENT '创建时间',
    `source_type` STRING COMMENT '来源类型',
    `source_id` STRING COMMENT '来源编号',
    `sku_num` BIGINT COMMENT '商品数量',
    `original_amount` DECIMAL(16,2) COMMENT '原始价格',
    `split_activity_amount` DECIMAL(16,2) COMMENT '活动优惠分摊',
    `split_coupon_amount` DECIMAL(16,2) COMMENT '优惠券优惠分摊',
    `split_final_amount` DECIMAL(16,2) COMMENT '最终价格分摊'
) COMMENT '订单明细事实表表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall/dwd/dwd_order_detail/'
TBLPROPERTIES ("parquet.compression"="lzo");

insert overwrite table dwd_order_detail partition(dt)
select
    od.id,
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
    od.order_price*od.sku_num,
    od.split_activity_amount,
    od.split_coupon_amount,
    od.split_final_amount,
    date_format(create_time,'yyyy-MM-dd')
from
(
    select
        *
    from ods_order_detail
    where dt='2020-06-14'
)od
left join
(
    select
        id,
        user_id,
        province_id
    from ods_order_info
    where dt='2020-06-14'
)oi
on od.order_id=oi.id
left join
(
    select
        order_detail_id,
        activity_id,
        activity_rule_id
    from ods_order_detail_activity
    where dt='2020-06-14'
)oda
on od.id=oda.order_detail_id
left join
(
    select
        order_detail_id,
        coupon_id
    from ods_order_detail_coupon
    where dt='2020-06-14'
)odc
on od.id=odc.order_detail_id;

---------------------------------------dwd_order_refund_info----------------------------------
CREATE EXTERNAL TABLE dwd_order_refund_info(
    `id` STRING COMMENT '编号',
    `user_id` STRING COMMENT '用户ID',
    `order_id` STRING COMMENT '订单ID',
    `sku_id` STRING COMMENT '商品ID',
    `province_id` STRING COMMENT '地区ID',
    `refund_type` STRING COMMENT '退单类型',
    `refund_num` BIGINT COMMENT '退单件数',
    `refund_amount` DECIMAL(16,2) COMMENT '退单金额',
    `refund_reason_type` STRING COMMENT '退单原因类型',
    `create_time` STRING COMMENT '退单时间'
) COMMENT '退单事实表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall/dwd/dwd_order_refund_info/'
TBLPROPERTIES ("parquet.compression"="lzo");

insert overwrite table dwd_order_refund_info partition(dt)
select
    ri.id,
    ri.user_id,
    ri.order_id,
    ri.sku_id,
    oi.province_id,
    ri.refund_type,
    ri.refund_num,
    ri.refund_amount,
    ri.refund_reason_type,
    ri.create_time,
    date_format(ri.create_time,'yyyy-MM-dd')
from
(
    select * from ods_order_refund_info where dt='2020-06-14'
)ri
left join
(
    select id,province_id from ods_order_info where dt='2020-06-14'
)oi
on ri.order_id=oi.id;

---------------------------------------dwd_cart_info----------------------------------
CREATE EXTERNAL TABLE dwd_cart_info(
    `id` STRING COMMENT '编号',
    `user_id` STRING COMMENT '用户ID',
    `sku_id` STRING COMMENT '商品ID',
    `source_type` STRING COMMENT '来源类型',
    `source_id` STRING COMMENT '来源编号',
    `cart_price` DECIMAL(16,2) COMMENT '加入购物车时的价格',
    `is_ordered` STRING COMMENT '是否已下单',
    `create_time` STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间',
    `order_time` STRING COMMENT '下单时间',
    `sku_num` BIGINT COMMENT '加购数量'
) COMMENT '加购事实表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall/dwd/dwd_cart_info/'
TBLPROPERTIES ("parquet.compression"="lzo");

insert overwrite table dwd_cart_info partition(dt='2020-06-14')
select
    id,
    user_id,
    sku_id,
    source_type,
    source_id,
    cart_price,
    is_ordered,
    create_time,
    operate_time,
    order_time,
    sku_num
from ods_cart_info
where dt='2020-06-14';

---------------------------------------dwd_favor_info----------------------------------
CREATE EXTERNAL TABLE dwd_favor_info(
    `id` STRING COMMENT '编号',
    `user_id` STRING  COMMENT '用户id',
    `sku_id` STRING  COMMENT 'skuid',
    `spu_id` STRING  COMMENT 'spuid',
    `is_cancel` STRING  COMMENT '是否取消',
    `create_time` STRING  COMMENT '收藏时间',
    `cancel_time` STRING  COMMENT '取消时间'
) COMMENT '收藏事实表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall/dwd/dwd_favor_info/'
TBLPROPERTIES ("parquet.compression"="lzo");

insert overwrite table dwd_favor_info partition(dt='2020-06-14')
select
    id,
    user_id,
    sku_id,
    spu_id,
    is_cancel,
    create_time,
    cancel_time
from ods_favor_info
where dt='2020-06-14';

---------------------------------------dwd_coupon_use----------------------------------
CREATE EXTERNAL TABLE dwd_coupon_use(
    `id` STRING COMMENT '编号',
    `coupon_id` STRING  COMMENT '优惠券ID',
    `user_id` STRING  COMMENT 'userid',
    `order_id` STRING  COMMENT '订单id',
    `coupon_status` STRING  COMMENT '优惠券状态',
    `get_time` STRING  COMMENT '领取时间',
    `using_time` STRING  COMMENT '使用时间(下单)',
    `used_time` STRING  COMMENT '使用时间(支付)',
    `expire_time` STRING COMMENT '过期时间'
) COMMENT '优惠券领用事实表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall/dwd/dwd_coupon_use/'
TBLPROPERTIES ("parquet.compression"="lzo");

insert overwrite table dwd_coupon_use partition(dt)
select
    id,
    coupon_id,
    user_id,
    order_id,
    coupon_status,
    get_time,
    using_time,
    used_time,
    expire_time,
    coalesce(date_format(used_time,'yyyy-MM-dd'),date_format(expire_time,'yyyy-MM-dd'),'9999-99-99')
from ods_coupon_use
where dt='2020-06-14';

insert overwrite table dwd_coupon_use partition(dt)
select
    nvl(new.id,old.id),
    nvl(new.coupon_id,old.coupon_id),
    nvl(new.user_id,old.user_id),
    nvl(new.order_id,old.order_id),
    nvl(new.coupon_status,old.coupon_status),
    nvl(new.get_time,old.get_time),
    nvl(new.using_time,old.using_time),
    nvl(new.used_time,old.used_time),
    nvl(new.expire_time,old.expire_time),
    coalesce(date_format(nvl(new.used_time,old.used_time),'yyyy-MM-dd'),date_format(nvl(new.expire_time,old.expire_time),'yyyy-MM-dd'),'9999-99-99')
from
(
    select
        id,
        coupon_id,
        user_id,
        order_id,
        coupon_status,
        get_time,
        using_time,
        used_time,
        expire_time
    from dwd_coupon_use
    where dt='9999-99-99'
)old
full outer join
(
    select
        id,
        coupon_id,
        user_id,
        order_id,
        coupon_status,
        get_time,
        using_time,
        used_time,
        expire_time
    from ods_coupon_use
    where dt='2020-06-15'
)new
on old.id=new.id;

---------------------------------------dwd_payment_info----------------------------------
CREATE EXTERNAL TABLE dwd_payment_info (
    `id` STRING COMMENT '编号',
    `order_id` STRING COMMENT '订单编号',
    `user_id` STRING COMMENT '用户编号',
    `province_id` STRING COMMENT '地区ID',
    `trade_no` STRING COMMENT '交易编号',
    `out_trade_no` STRING COMMENT '对外交易编号',
    `payment_type` STRING COMMENT '支付类型',
    `payment_amount` DECIMAL(16,2) COMMENT '支付金额',
    `payment_status` STRING COMMENT '支付状态',
    `create_time` STRING COMMENT '创建时间',--调用第三方支付接口的时间
    `callback_time` STRING COMMENT '完成时间'--支付完成时间，即支付成功回调时间
) COMMENT '支付事实表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall/dwd/dwd_payment_info/'
TBLPROPERTIES ("parquet.compression"="lzo");

insert overwrite table dwd_payment_info partition(dt)
select
    pi.id,
    pi.order_id,
    pi.user_id,
    oi.province_id,
    pi.trade_no,
    pi.out_trade_no,
    pi.payment_type,
    pi.payment_amount,
    pi.payment_status,
    pi.create_time,
    pi.callback_time,
    nvl(date_format(pi.callback_time,'yyyy-MM-dd'),'9999-99-99')
from
(
    select * from ods_payment_info where dt='2020-06-14'
)pi
left join
(
    select id,province_id from ods_order_info where dt='2020-06-14'
)oi
on pi.order_id=oi.id;

insert overwrite table dwd_payment_info partition(dt)
select
    nvl(new.id,old.id),
    nvl(new.order_id,old.order_id),
    nvl(new.user_id,old.user_id),
    nvl(new.province_id,old.province_id),
    nvl(new.trade_no,old.trade_no),
    nvl(new.out_trade_no,old.out_trade_no),
    nvl(new.payment_type,old.payment_type),
    nvl(new.payment_amount,old.payment_amount),
    nvl(new.payment_status,old.payment_status),
    nvl(new.create_time,old.create_time),
    nvl(new.callback_time,old.callback_time),
    nvl(date_format(nvl(new.callback_time,old.callback_time),'yyyy-MM-dd'),'9999-99-99')
from
(
    select id,
       order_id,
       user_id,
       province_id,
       trade_no,
       out_trade_no,
       payment_type,
       payment_amount,
       payment_status,
       create_time,
       callback_time
    from dwd_payment_info
    where dt = '9999-99-99'
)old
full outer join
(
    select
        pi.id,
        pi.out_trade_no,
        pi.order_id,
        pi.user_id,
        oi.province_id,
        pi.payment_type,
        pi.trade_no,
        pi.payment_amount,
        pi.payment_status,
        pi.create_time,
        pi.callback_time
    from
    (
        select * from ods_payment_info where dt='2020-06-15'
    )pi
    left join
    (
        select id,province_id from ods_order_info where dt='2020-06-15'
    )oi
    on pi.order_id=oi.id
)new
on old.id=new.id;

---------------------------------------dwd_refund_payment----------------------------------
CREATE EXTERNAL TABLE dwd_refund_payment (
    `id` STRING COMMENT '编号',
    `user_id` STRING COMMENT '用户ID',
    `order_id` STRING COMMENT '订单编号',
    `sku_id` STRING COMMENT 'SKU编号',
    `province_id` STRING COMMENT '地区ID',
    `trade_no` STRING COMMENT '交易编号',
    `out_trade_no` STRING COMMENT '对外交易编号',
    `payment_type` STRING COMMENT '支付类型',
    `refund_amount` DECIMAL(16,2) COMMENT '退款金额',
    `refund_status` STRING COMMENT '退款状态',
    `create_time` STRING COMMENT '创建时间',--调用第三方支付接口的时间
    `callback_time` STRING COMMENT '回调时间'--支付接口回调时间，即支付成功时间
) COMMENT '退款事实表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall/dwd/dwd_refund_payment/'
TBLPROPERTIES ("parquet.compression"="lzo");

insert overwrite table dwd_refund_payment partition(dt)
select
    rp.id,
    user_id,
    order_id,
    sku_id,
    province_id,
    trade_no,
    out_trade_no,
    payment_type,
    refund_amount,
    refund_status,
    create_time,
    callback_time,
    nvl(date_format(callback_time,'yyyy-MM-dd'),'9999-99-99')
from
(
    select
        id,
        out_trade_no,
        order_id,
        sku_id,
        payment_type,
        trade_no,
        refund_amount,
        refund_status,
        create_time,
        callback_time
    from ods_refund_payment
    where dt='2020-06-14'
)rp
left join
(
    select
        id,
        user_id,
        province_id
    from ods_order_info
    where dt='2020-06-14'
)oi
on rp.order_id=oi.id;

insert overwrite table dwd_refund_payment partition(dt)
select
    nvl(new.id,old.id),
    nvl(new.user_id,old.user_id),
    nvl(new.order_id,old.order_id),
    nvl(new.sku_id,old.sku_id),
    nvl(new.province_id,old.province_id),
    nvl(new.trade_no,old.trade_no),
    nvl(new.out_trade_no,old.out_trade_no),
    nvl(new.payment_type,old.payment_type),
    nvl(new.refund_amount,old.refund_amount),
    nvl(new.refund_status,old.refund_status),
    nvl(new.create_time,old.create_time),
    nvl(new.callback_time,old.callback_time),
    nvl(date_format(nvl(new.callback_time,old.callback_time),'yyyy-MM-dd'),'9999-99-99')
from
(
    select
        id,
        user_id,
        order_id,
        sku_id,
        province_id,
        trade_no,
        out_trade_no,
        payment_type,
        refund_amount,
        refund_status,
        create_time,
        callback_time
    from dwd_refund_payment
    where dt='9999-99-99'
)old
full outer join
(
    select
        rp.id,
        user_id,
        order_id,
        sku_id,
        province_id,
        trade_no,
        out_trade_no,
        payment_type,
        refund_amount,
        refund_status,
        create_time,
        callback_time
    from
    (
        select
            id,
            out_trade_no,
            order_id,
            sku_id,
            payment_type,
            trade_no,
            refund_amount,
            refund_status,
            create_time,
            callback_time
        from ods_refund_payment
        where dt='2020-06-15'
    )rp
    left join
    (
        select
            id,
            user_id,
            province_id
        from ods_order_info
        where dt='2020-06-15'
    )oi
    on rp.order_id=oi.id
)new
on old.id=new.id;

---------------------------------------dws_visitor_action_daycount----------------------------------
CREATE EXTERNAL TABLE dws_visitor_action_daycount
(
    `mid_id` STRING COMMENT '设备id',
    `brand` STRING COMMENT '设备品牌',
    `model` STRING COMMENT '设备型号',
    `is_new` STRING COMMENT '是否首次访问',
    `channel` ARRAY<STRING> COMMENT '渠道',
    `os` ARRAY<STRING> COMMENT '操作系统',
    `area_code` ARRAY<STRING> COMMENT '地区ID',
    `version_code` ARRAY<STRING> COMMENT '应用版本',
    `visit_count` BIGINT COMMENT '访问次数',
    `page_stats` ARRAY<STRUCT<page_id:STRING,page_count:BIGINT,during_time:BIGINT>> COMMENT '页面访问统计'
) COMMENT '每日设备行为表'
PARTITIONED BY(`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall/dws/dws_visitor_action_daycount'
TBLPROPERTIES ("parquet.compression"="lzo");

insert overwrite table dws_visitor_action_daycount partition(dt='2020-06-14')
select
    t1.mid_id,
    t1.brand,
    t1.model,
    t1.is_new,
    t1.channel,
    t1.os,
    t1.area_code,
    t1.version_code,
    t1.visit_count,
    t3.page_stats
from
(
    select
        mid_id,
        brand,
        model,
        if(array_contains(collect_set(is_new),'0'),'0','1') is_new,--ods_page_log中，同一天内，同一设备的is_new字段，可能全部为1，可能全部为0，也可能部分为0，部分为1(卸载重装),故做该处理
        collect_set(channel) channel,
        collect_set(os) os,
        collect_set(area_code) area_code,
        collect_set(version_code) version_code,
        sum(if(last_page_id is null,1,0)) visit_count
    from dwd_page_log
    where dt='2020-06-14'
    and last_page_id is null
    group by mid_id,model,brand
)t1
join
(
    select
        mid_id,
        brand,
        model,
        collect_set(named_struct('page_id',page_id,'page_count',page_count,'during_time',during_time)) page_stats
    from
    (
        select
            mid_id,
            brand,
            model,
            page_id,
            count(*) page_count,
            sum(during_time) during_time
        from dwd_page_log
        where dt='2020-06-14'
        group by mid_id,model,brand,page_id
    )t2
    group by mid_id,model,brand
)t3
on t1.mid_id=t3.mid_id
and t1.brand=t3.brand
and t1.model=t3.model;

---------------------------------------dws_user_action_daycount----------------------------------
CREATE EXTERNAL TABLE dws_user_action_daycount
(
    `user_id` STRING COMMENT '用户id',
    `login_count` BIGINT COMMENT '登录次数',
    `cart_count` BIGINT COMMENT '加入购物车次数',
    `favor_count` BIGINT COMMENT '收藏次数',
    `order_count` BIGINT COMMENT '下单次数',
    `order_activity_count` BIGINT COMMENT '订单参与活动次数',
    `order_activity_reduce_amount` DECIMAL(16,2) COMMENT '订单减免金额(活动)',
    `order_coupon_count` BIGINT COMMENT '订单用券次数',
    `order_coupon_reduce_amount` DECIMAL(16,2) COMMENT '订单减免金额(优惠券)',
    `order_original_amount` DECIMAL(16,2)  COMMENT '订单单原始金额',
    `order_final_amount` DECIMAL(16,2) COMMENT '订单总金额',
    `payment_count` BIGINT COMMENT '支付次数',
    `payment_amount` DECIMAL(16,2) COMMENT '支付金额',
    `refund_order_count` BIGINT COMMENT '退单次数',
    `refund_order_num` BIGINT COMMENT '退单件数',
    `refund_order_amount` DECIMAL(16,2) COMMENT '退单金额',
    `refund_payment_count` BIGINT COMMENT '退款次数',
    `refund_payment_num` BIGINT COMMENT '退款件数',
    `refund_payment_amount` DECIMAL(16,2) COMMENT '退款金额',
    `coupon_get_count` BIGINT COMMENT '优惠券领取次数',
    `coupon_using_count` BIGINT COMMENT '优惠券使用(下单)次数',
    `coupon_used_count` BIGINT COMMENT '优惠券使用(支付)次数',
    `appraise_good_count` BIGINT COMMENT '好评数',
    `appraise_mid_count` BIGINT COMMENT '中评数',
    `appraise_bad_count` BIGINT COMMENT '差评数',
    `appraise_default_count` BIGINT COMMENT '默认评价数',
    `order_detail_stats` array<struct<sku_id:string,sku_num:bigint,order_count:bigint,activity_reduce_amount:decimal(16,2),coupon_reduce_amount:decimal(16,2),original_amount:decimal(16,2),final_amount:decimal(16,2)>> COMMENT '下单明细统计'
) COMMENT '每日用户行为'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall/dws/dws_user_action_daycount/'
TBLPROPERTIES ("parquet.compression"="lzo");

with
tmp_login as
(
    select
        dt,
        user_id,
        count(*) login_count
    from dwd_page_log
    where user_id is not null
    and last_page_id is null
    group by dt,user_id
),
tmp_cf as
(
    select
        dt,
        user_id,
        sum(if(action_id='cart_add',1,0)) cart_count,
        sum(if(action_id='favor_add',1,0)) favor_count
    from dwd_action_log
    where user_id is not null
    and action_id in ('cart_add','favor_add')
    group by dt,user_id
),
tmp_order as
(
    select
        date_format(create_time,'yyyy-MM-dd') dt,
        user_id,
        count(*) order_count,
        sum(if(activity_reduce_amount>0,1,0)) order_activity_count,
        sum(if(coupon_reduce_amount>0,1,0)) order_coupon_count,
        sum(activity_reduce_amount) order_activity_reduce_amount,
        sum(coupon_reduce_amount) order_coupon_reduce_amount,
        sum(original_amount) order_original_amount,
        sum(final_amount) order_final_amount
    from dwd_order_info
    group by date_format(create_time,'yyyy-MM-dd'),user_id
),
tmp_pay as
(
    select
        date_format(callback_time,'yyyy-MM-dd') dt,
        user_id,
        count(*) payment_count,
        sum(payment_amount) payment_amount
    from dwd_payment_info
    group by date_format(callback_time,'yyyy-MM-dd'),user_id
),
tmp_ri as
(
    select
        date_format(create_time,'yyyy-MM-dd') dt,
        user_id,
        count(*) refund_order_count,
        sum(refund_num) refund_order_num,
        sum(refund_amount) refund_order_amount
    from dwd_order_refund_info
    group by date_format(create_time,'yyyy-MM-dd'),user_id
),
tmp_rp as
(
    select
        date_format(callback_time,'yyyy-MM-dd') dt,
        rp.user_id,
        count(*) refund_payment_count,
        sum(ri.refund_num) refund_payment_num,
        sum(rp.refund_amount) refund_payment_amount
    from
    (
        select
            user_id,
            order_id,
            sku_id,
            refund_amount,
            callback_time
        from dwd_refund_payment
    )rp
    left join
    (
        select
            user_id,
            order_id,
            sku_id,
            refund_num
        from dwd_order_refund_info
    )ri
    on rp.order_id=ri.order_id
    and rp.sku_id=ri.sku_id
    group by date_format(callback_time,'yyyy-MM-dd'),rp.user_id
),
tmp_coupon as
(
    select
        coalesce(coupon_get.dt,coupon_using.dt,coupon_used.dt) dt,
        coalesce(coupon_get.user_id,coupon_using.user_id,coupon_used.user_id) user_id,
        nvl(coupon_get_count,0) coupon_get_count,
        nvl(coupon_using_count,0) coupon_using_count,
        nvl(coupon_used_count,0) coupon_used_count
    from
    (
        select
            date_format(get_time,'yyyy-MM-dd') dt,
            user_id,
            count(*) coupon_get_count
        from dwd_coupon_use
        where get_time is not null
        group by user_id,date_format(get_time,'yyyy-MM-dd')
    )coupon_get
    full outer join
    (
        select
            date_format(using_time,'yyyy-MM-dd') dt,
            user_id,
            count(*) coupon_using_count
        from dwd_coupon_use
        where using_time is not null
        group by user_id,date_format(using_time,'yyyy-MM-dd')
    )coupon_using
    on coupon_get.dt=coupon_using.dt
    and coupon_get.user_id=coupon_using.user_id
    full outer join
    (
        select
            date_format(used_time,'yyyy-MM-dd') dt,
            user_id,
            count(*) coupon_used_count
        from dwd_coupon_use
        where used_time is not null
        group by user_id,date_format(used_time,'yyyy-MM-dd')
    )coupon_used
    on nvl(coupon_get.dt,coupon_using.dt)=coupon_used.dt
    and nvl(coupon_get.user_id,coupon_using.user_id)=coupon_used.user_id
),
tmp_comment as
(
    select
        date_format(create_time,'yyyy-MM-dd') dt,
        user_id,
        sum(if(appraise='1201',1,0)) appraise_good_count,
        sum(if(appraise='1202',1,0)) appraise_mid_count,
        sum(if(appraise='1203',1,0)) appraise_bad_count,
        sum(if(appraise='1204',1,0)) appraise_default_count
    from dwd_comment_info
    group by date_format(create_time,'yyyy-MM-dd'),user_id
),
tmp_od as
(
    select
        dt,
        user_id,
        collect_set(named_struct('sku_id',sku_id,'sku_num',sku_num,'order_count',order_count,'activity_reduce_amount',activity_reduce_amount,'coupon_reduce_amount',coupon_reduce_amount,'original_amount',original_amount,'final_amount',final_amount)) order_detail_stats
    from
    (
        select
            date_format(create_time,'yyyy-MM-dd') dt,
            user_id,
            sku_id,
            sum(sku_num) sku_num,
            count(*) order_count,
            cast(sum(split_activity_amount) as decimal(16,2)) activity_reduce_amount,
            cast(sum(split_coupon_amount) as decimal(16,2)) coupon_reduce_amount,
            cast(sum(original_amount) as decimal(16,2)) original_amount,
            cast(sum(split_final_amount) as decimal(16,2)) final_amount
        from dwd_order_detail
        group by date_format(create_time,'yyyy-MM-dd'),user_id,sku_id
    )t1
    group by dt,user_id
)

insert overwrite table dws_user_action_daycount partition(dt)
select
    coalesce(tmp_login.user_id,tmp_cf.user_id,tmp_order.user_id,tmp_pay.user_id,tmp_ri.user_id,tmp_rp.user_id,tmp_comment.user_id,tmp_coupon.user_id,tmp_od.user_id),
    nvl(login_count,0),
    nvl(cart_count,0),
    nvl(favor_count,0),
    nvl(order_count,0),
    nvl(order_activity_count,0),
    nvl(order_activity_reduce_amount,0),
    nvl(order_coupon_count,0),
    nvl(order_coupon_reduce_amount,0),
    nvl(order_original_amount,0),
    nvl(order_final_amount,0),
    nvl(payment_count,0),
    nvl(payment_amount,0),
    nvl(refund_order_count,0),
    nvl(refund_order_num,0),
    nvl(refund_order_amount,0),
    nvl(refund_payment_count,0),
    nvl(refund_payment_num,0),
    nvl(refund_payment_amount,0),
    nvl(coupon_get_count,0),
    nvl(coupon_using_count,0),
    nvl(coupon_used_count,0),
    nvl(appraise_good_count,0),
    nvl(appraise_mid_count,0),
    nvl(appraise_bad_count,0),
    nvl(appraise_default_count,0),
    order_detail_stats,
    coalesce(tmp_login.dt,tmp_cf.dt,tmp_order.dt,tmp_pay.dt,tmp_ri.dt,tmp_rp.dt,tmp_comment.dt,tmp_coupon.dt,tmp_od.dt)
from tmp_login
full outer join tmp_cf
on tmp_login.user_id=tmp_cf.user_id
and tmp_login.dt=tmp_cf.dt
full outer join tmp_order
on coalesce(tmp_login.user_id,tmp_cf.user_id)=tmp_order.user_id
and coalesce(tmp_login.dt,tmp_cf.dt)=tmp_order.dt
full outer join tmp_pay
on coalesce(tmp_login.user_id,tmp_cf.user_id,tmp_order.user_id)=tmp_pay.user_id
and coalesce(tmp_login.dt,tmp_cf.dt,tmp_order.dt)=tmp_pay.dt
full outer join tmp_ri
on coalesce(tmp_login.user_id,tmp_cf.user_id,tmp_order.user_id,tmp_pay.user_id)=tmp_ri.user_id
and coalesce(tmp_login.dt,tmp_cf.dt,tmp_order.dt,tmp_pay.dt)=tmp_ri.dt
full outer join tmp_rp
on coalesce(tmp_login.user_id,tmp_cf.user_id,tmp_order.user_id,tmp_pay.user_id,tmp_ri.user_id)=tmp_rp.user_id
and coalesce(tmp_login.dt,tmp_cf.dt,tmp_order.dt,tmp_pay.dt,tmp_ri.dt)=tmp_rp.dt
full outer join tmp_comment
on coalesce(tmp_login.user_id,tmp_cf.user_id,tmp_order.user_id,tmp_pay.user_id,tmp_ri.user_id,tmp_rp.user_id)=tmp_comment.user_id
and coalesce(tmp_login.dt,tmp_cf.dt,tmp_order.dt,tmp_pay.dt,tmp_ri.dt,tmp_rp.dt)=tmp_comment.dt
full outer join tmp_coupon
on coalesce(tmp_login.user_id,tmp_cf.user_id,tmp_order.user_id,tmp_pay.user_id,tmp_ri.user_id,tmp_rp.user_id,tmp_comment.user_id)=tmp_coupon.user_id
and coalesce(tmp_login.dt,tmp_cf.dt,tmp_order.dt,tmp_pay.dt,tmp_ri.dt,tmp_rp.dt,tmp_comment.dt)=tmp_coupon.dt
full outer join tmp_od
on coalesce(tmp_login.user_id,tmp_cf.user_id,tmp_order.user_id,tmp_pay.user_id,tmp_ri.user_id,tmp_rp.user_id,tmp_comment.user_id,tmp_coupon.user_id)=tmp_od.user_id
and coalesce(tmp_login.dt,tmp_cf.dt,tmp_order.dt,tmp_pay.dt,tmp_ri.dt,tmp_rp.dt,tmp_comment.dt,tmp_coupon.dt)=tmp_od.dt;

---------------------------------------dws_sku_action_daycount----------------------------------
CREATE EXTERNAL TABLE dws_sku_action_daycount
(
    `sku_id` STRING COMMENT 'sku_id',
    `order_count` BIGINT COMMENT '被下单次数',
    `order_num` BIGINT COMMENT '被下单件数',
    `order_activity_count` BIGINT COMMENT '参与活动被下单次数',
    `order_coupon_count` BIGINT COMMENT '使用优惠券被下单次数',
    `order_activity_reduce_amount` DECIMAL(16,2) COMMENT '优惠金额(活动)',
    `order_coupon_reduce_amount` DECIMAL(16,2) COMMENT '优惠金额(优惠券)',
    `order_original_amount` DECIMAL(16,2) COMMENT '被下单原价金额',
    `order_final_amount` DECIMAL(16,2) COMMENT '被下单最终金额',
    `payment_count` BIGINT COMMENT '被支付次数',
    `payment_num` BIGINT COMMENT '被支付件数',
    `payment_amount` DECIMAL(16,2) COMMENT '被支付金额',
    `refund_order_count` BIGINT  COMMENT '被退单次数',
    `refund_order_num` BIGINT COMMENT '被退单件数',
    `refund_order_amount` DECIMAL(16,2) COMMENT '被退单金额',
    `refund_payment_count` BIGINT  COMMENT '被退款次数',
    `refund_payment_num` BIGINT COMMENT '被退款件数',
    `refund_payment_amount` DECIMAL(16,2) COMMENT '被退款金额',
    `cart_count` BIGINT COMMENT '被加入购物车次数',
    `favor_count` BIGINT COMMENT '被收藏次数',
    `appraise_good_count` BIGINT COMMENT '好评数',
    `appraise_mid_count` BIGINT COMMENT '中评数',
    `appraise_bad_count` BIGINT COMMENT '差评数',
    `appraise_default_count` BIGINT COMMENT '默认评价数'
) COMMENT '每日商品行为'
PARTITIONED BY (`dt` STRING)
STORED AS ORC
LOCATION '/warehouse/gmall/dws/dws_sku_action_daycount/'
TBLPROPERTIES ("orc.compress"="snappy");

with
tmp_order as
(
    select
        date_format(create_time,'yyyy-MM-dd') dt,
        sku_id,
        count(*) order_count,
        sum(sku_num) order_num,
        sum(if(split_activity_amount>0,1,0)) order_activity_count,
        sum(if(split_coupon_amount>0,1,0)) order_coupon_count,
        sum(split_activity_amount) order_activity_reduce_amount,
        sum(split_coupon_amount) order_coupon_reduce_amount,
        sum(original_amount) order_original_amount,
        sum(split_final_amount) order_final_amount
    from dwd_order_detail
    group by date_format(create_time,'yyyy-MM-dd'),sku_id
),
tmp_pay as
(
    select
        date_format(callback_time,'yyyy-MM-dd') dt,
        sku_id,
        count(*) payment_count,
        sum(sku_num) payment_num,
        sum(split_final_amount) payment_amount
    from dwd_order_detail od
    join
    (
        select
            order_id,
            callback_time
        from dwd_payment_info
        where callback_time is not null
    )pi on pi.order_id=od.order_id
    group by date_format(callback_time,'yyyy-MM-dd'),sku_id
),
tmp_ri as
(
    select
        date_format(create_time,'yyyy-MM-dd') dt,
        sku_id,
        count(*) refund_order_count,
        sum(refund_num) refund_order_num,
        sum(refund_amount) refund_order_amount
    from dwd_order_refund_info
    group by date_format(create_time,'yyyy-MM-dd'),sku_id
),
tmp_rp as
(
    select
        date_format(callback_time,'yyyy-MM-dd') dt,
        rp.sku_id,
        count(*) refund_payment_count,
        sum(ri.refund_num) refund_payment_num,
        sum(refund_amount) refund_payment_amount
    from
    (
        select
            order_id,
            sku_id,
            refund_amount,
            callback_time
        from dwd_refund_payment
    )rp
    left join
    (
        select
            order_id,
            sku_id,
            refund_num
        from dwd_order_refund_info
    )ri
    on rp.order_id=ri.order_id
    and rp.sku_id=ri.sku_id
    group by date_format(callback_time,'yyyy-MM-dd'),rp.sku_id
),
tmp_cf as
(
    select
        dt,
        item sku_id,
        sum(if(action_id='cart_add',1,0)) cart_count,
        sum(if(action_id='favor_add',1,0)) favor_count
    from dwd_action_log
    where action_id in ('cart_add','favor_add')
    group by dt,item
),
tmp_comment as
(
    select
        date_format(create_time,'yyyy-MM-dd') dt,
        sku_id,
        sum(if(appraise='1201',1,0)) appraise_good_count,
        sum(if(appraise='1202',1,0)) appraise_mid_count,
        sum(if(appraise='1203',1,0)) appraise_bad_count,
        sum(if(appraise='1204',1,0)) appraise_default_count
    from dwd_comment_info
    group by date_format(create_time,'yyyy-MM-dd'),sku_id
)
insert overwrite table dws_sku_action_daycount partition(dt)
select
    sku_id,
    sum(order_count),
    sum(order_num),
    sum(order_activity_count),
    sum(order_coupon_count),
    sum(order_activity_reduce_amount),
    sum(order_coupon_reduce_amount),
    sum(order_original_amount),
    sum(order_final_amount),
    sum(payment_count),
    sum(payment_num),
    sum(payment_amount),
    sum(refund_order_count),
    sum(refund_order_num),
    sum(refund_order_amount),
    sum(refund_payment_count),
    sum(refund_payment_num),
    sum(refund_payment_amount),
    sum(cart_count),
    sum(favor_count),
    sum(appraise_good_count),
    sum(appraise_mid_count),
    sum(appraise_bad_count),
    sum(appraise_default_count),
    dt
from
(
    select
        dt,
        sku_id,
        order_count,
        order_num,
        order_activity_count,
        order_coupon_count,
        order_activity_reduce_amount,
        order_coupon_reduce_amount,
        order_original_amount,
        order_final_amount,
        0 payment_count,
        0 payment_num,
        0 payment_amount,
        0 refund_order_count,
        0 refund_order_num,
        0 refund_order_amount,
        0 refund_payment_count,
        0 refund_payment_num,
        0 refund_payment_amount,
        0 cart_count,
        0 favor_count,
        0 appraise_good_count,
        0 appraise_mid_count,
        0 appraise_bad_count,
        0 appraise_default_count
    from tmp_order
    union all
    select
        dt,
        sku_id,
        0 order_count,
        0 order_num,
        0 order_activity_count,
        0 order_coupon_count,
        0 order_activity_reduce_amount,
        0 order_coupon_reduce_amount,
        0 order_original_amount,
        0 order_final_amount,
        payment_count,
        payment_num,
        payment_amount,
        0 refund_order_count,
        0 refund_order_num,
        0 refund_order_amount,
        0 refund_payment_count,
        0 refund_payment_num,
        0 refund_payment_amount,
        0 cart_count,
        0 favor_count,
        0 appraise_good_count,
        0 appraise_mid_count,
        0 appraise_bad_count,
        0 appraise_default_count
    from tmp_pay
    union all
    select
        dt,
        sku_id,
        0 order_count,
        0 order_num,
        0 order_activity_count,
        0 order_coupon_count,
        0 order_activity_reduce_amount,
        0 order_coupon_reduce_amount,
        0 order_original_amount,
        0 order_final_amount,
        0 payment_count,
        0 payment_num,
        0 payment_amount,
        refund_order_count,
        refund_order_num,
        refund_order_amount,
        0 refund_payment_count,
        0 refund_payment_num,
        0 refund_payment_amount,
        0 cart_count,
        0 favor_count,
        0 appraise_good_count,
        0 appraise_mid_count,
        0 appraise_bad_count,
        0 appraise_default_count
    from tmp_ri
    union all
    select
        dt,
        sku_id,
        0 order_count,
        0 order_num,
        0 order_activity_count,
        0 order_coupon_count,
        0 order_activity_reduce_amount,
        0 order_coupon_reduce_amount,
        0 order_original_amount,
        0 order_final_amount,
        0 payment_count,
        0 payment_num,
        0 payment_amount,
        0 refund_order_count,
        0 refund_order_num,
        0 refund_order_amount,
        refund_payment_count,
        refund_payment_num,
        refund_payment_amount,
        0 cart_count,
        0 favor_count,
        0 appraise_good_count,
        0 appraise_mid_count,
        0 appraise_bad_count,
        0 appraise_default_count
    from tmp_rp
    union all
    select
        dt,
        sku_id,
        0 order_count,
        0 order_num,
        0 order_activity_count,
        0 order_coupon_count,
        0 order_activity_reduce_amount,
        0 order_coupon_reduce_amount,
        0 order_original_amount,
        0 order_final_amount,
        0 payment_count,
        0 payment_num,
        0 payment_amount,
        0 refund_order_count,
        0 refund_order_num,
        0 refund_order_amount,
        0 refund_payment_count,
        0 refund_payment_num,
        0 refund_payment_amount,
        cart_count,
        favor_count,
        0 appraise_good_count,
        0 appraise_mid_count,
        0 appraise_bad_count,
        0 appraise_default_count
    from tmp_cf
    union all
    select
        dt,
        sku_id,
        0 order_count,
        0 order_num,
        0 order_activity_count,
        0 order_coupon_count,
        0 order_activity_reduce_amount,
        0 order_coupon_reduce_amount,
        0 order_original_amount,
        0 order_final_amount,
        0 payment_count,
        0 payment_num,
        0 payment_amount,
        0 refund_order_count,
        0 refund_order_num,
        0 refund_order_amount,
        0 refund_payment_count,
        0 refund_payment_num,
        0 refund_payment_amount,
        0 cart_count,
        0 favor_count,
        appraise_good_count,
        appraise_mid_count,
        appraise_bad_count,
        appraise_default_count
    from tmp_comment
)t1
group by dt,sku_id;

---------------------------------------dws_coupon_info_daycount----------------------------------
DROP TABLE IF EXISTS dws_coupon_info_daycount;
CREATE EXTERNAL TABLE dws_coupon_info_daycount(
    `coupon_id` STRING COMMENT '优惠券ID',
    `get_count` BIGINT COMMENT '领取次数',
    `order_count` BIGINT COMMENT '使用(下单)次数',
    `order_reduce_amount` DECIMAL(16,2) COMMENT '使用某券的订单优惠金额',
    `order_original_amount` DECIMAL(16,2) COMMENT '使用某券的订单原价金额',
    `order_final_amount` DECIMAL(16,2) COMMENT '使用某券的订单总价金额',
    `payment_count` BIGINT COMMENT '使用(支付)次数',
    `payment_reduce_amount` DECIMAL(16,2) COMMENT '使用某券的支付优惠金额',
    `payment_amount` DECIMAL(16,2) COMMENT '使用某券支付的总金额',
    `expire_count` BIGINT COMMENT '过期次数'
) COMMENT '每日活动统计'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall/dws/dws_coupon_info_daycount/'
TBLPROPERTIES ("parquet.compression"="lzo");

with
tmp_cu as
(
    select
        coalesce(coupon_get.dt,coupon_using.dt,coupon_used.dt,coupon_exprie.dt) dt,
        coalesce(coupon_get.coupon_id,coupon_using.coupon_id,coupon_used.coupon_id,coupon_exprie.coupon_id) coupon_id,
        nvl(get_count,0) get_count,
        nvl(order_count,0) order_count,
        nvl(payment_count,0) payment_count,
        nvl(expire_count,0) expire_count
    from
    (
        select
            date_format(get_time,'yyyy-MM-dd') dt,
            coupon_id,
            count(*) get_count
        from dwd_coupon_use
        group by date_format(get_time,'yyyy-MM-dd'),coupon_id
    )coupon_get
    full outer join
    (
        select
            date_format(using_time,'yyyy-MM-dd') dt,
            coupon_id,
            count(*) order_count
        from dwd_coupon_use
        where using_time is not null
        group by date_format(using_time,'yyyy-MM-dd'),coupon_id
    )coupon_using
    on coupon_get.dt=coupon_using.dt
    and coupon_get.coupon_id=coupon_using.coupon_id
    full outer join
    (
        select
            date_format(used_time,'yyyy-MM-dd') dt,
            coupon_id,
            count(*) payment_count
        from dwd_coupon_use
        where used_time is not null
        group by date_format(used_time,'yyyy-MM-dd'),coupon_id
    )coupon_used
    on nvl(coupon_get.dt,coupon_using.dt)=coupon_used.dt
    and nvl(coupon_get.coupon_id,coupon_using.coupon_id)=coupon_used.coupon_id
    full outer join
    (
        select
            date_format(expire_time,'yyyy-MM-dd') dt,
            coupon_id,
            count(*) expire_count
        from dwd_coupon_use
        where expire_time is not null
        group by date_format(expire_time,'yyyy-MM-dd'),coupon_id
    )coupon_exprie
    on coalesce(coupon_get.dt,coupon_using.dt,coupon_used.dt)=coupon_exprie.dt
    and coalesce(coupon_get.coupon_id,coupon_using.coupon_id,coupon_used.coupon_id)=coupon_exprie.coupon_id
),
tmp_order as
(
    select
        date_format(create_time,'yyyy-MM-dd') dt,
        coupon_id,
        sum(split_coupon_amount) order_reduce_amount,
        sum(original_amount) order_original_amount,
        sum(split_final_amount) order_final_amount
    from dwd_order_detail
    where coupon_id is not null
    group by date_format(create_time,'yyyy-MM-dd'),coupon_id
),
tmp_pay as
(
    select
        date_format(callback_time,'yyyy-MM-dd') dt,
        coupon_id,
        sum(split_coupon_amount) payment_reduce_amount,
        sum(split_final_amount) payment_amount
    from
    (
        select
            order_id,
            coupon_id,
            split_coupon_amount,
            split_final_amount
        from dwd_order_detail
        where coupon_id is not null
    )od
    join
    (
        select
            order_id,
            callback_time
        from dwd_payment_info
    )pi
    on od.order_id=pi.order_id
    group by date_format(callback_time,'yyyy-MM-dd'),coupon_id
)
insert overwrite table dws_coupon_info_daycount partition(dt)
select
    coupon_id,
    sum(get_count),
    sum(order_count),
    sum(order_reduce_amount),
    sum(order_original_amount),
    sum(order_final_amount),
    sum(payment_count),
    sum(payment_reduce_amount),
    sum(payment_amount),
    sum(expire_count),
    dt
from
(
    select
        dt,
        coupon_id,
        get_count,
        order_count,
        0 order_reduce_amount,
        0 order_original_amount,
        0 order_final_amount,
        payment_count,
        0 payment_reduce_amount,
        0 payment_amount,
        expire_count
    from tmp_cu
    union all
    select
        dt,
        coupon_id,
        0 get_count,
        0 order_count,
        order_reduce_amount,
        order_original_amount,
        order_final_amount,
        0 payment_count,
        0 payment_reduce_amount,
        0 payment_amount,
        0 expire_count
    from tmp_order
    union all
    select
        dt,
        coupon_id,
        0 get_count,
        0 order_count,
        0 order_reduce_amount,
        0 order_original_amount,
        0 order_final_amount,
        0 payment_count,
        payment_reduce_amount,
        payment_amount,
        0 expire_count
    from tmp_pay
)t1
group by dt,coupon_id;

---------------------------------------dws_coupon_info_daycount----------------------------------
DROP TABLE IF EXISTS dws_activity_info_daycount;
CREATE EXTERNAL TABLE dws_activity_info_daycount(
    `activity_rule_id` STRING COMMENT '活动规则ID',
    `activity_id` STRING COMMENT '活动ID',
    `order_count` BIGINT COMMENT '参与某活动某规则下单次数',--注意：针对的是某个活动的某个具体规则
    `order_reduce_amount` DECIMAL(16,2) COMMENT '参与某活动某规则下单减免金额',
    `order_original_amount` DECIMAL(16,2) COMMENT '参与某活动某规则下单原始金额',--只统计参与活动的订单明细,未参与活动的订单明细不统计。
    `order_final_amount` DECIMAL(16,2) COMMENT '参与某活动某规则下单最终金额',
    `payment_count` BIGINT COMMENT '参与某活动某规则支付次数',
    `payment_reduce_amount` DECIMAL(16,2) COMMENT '参与某活动某规则支付减免金额',
    `payment_amount` DECIMAL(16,2) COMMENT '参与某活动某规则支付金额'
) COMMENT '每日活动统计'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall/dws/dws_activity_info_daycount/'
TBLPROPERTIES ("parquet.compression"="lzo");

with
tmp_order as
(
    select
        date_format(create_time,'yyyy-MM-dd') dt,
        activity_rule_id,
        activity_id,
        count(*) order_count,
        sum(split_activity_amount) order_reduce_amount,
        sum(original_amount) order_original_amount,
        sum(split_final_amount) order_final_amount
    from dwd_order_detail
    where activity_id is not null
    group by date_format(create_time,'yyyy-MM-dd'),activity_rule_id,activity_id
),
tmp_pay as
(
    select
        date_format(callback_time,'yyyy-MM-dd') dt,
        activity_rule_id,
        activity_id,
        count(*) payment_count,
        sum(split_activity_amount) payment_reduce_amount,
        sum(split_final_amount) payment_amount
    from
    (
        select
            activity_rule_id,
            activity_id,
            order_id,
            split_activity_amount,
            split_final_amount
        from dwd_order_detail
        where activity_id is not null
    )od
    join
    (
        select
            order_id,
            callback_time
        from dwd_payment_info
    )pi
    on od.order_id=pi.order_id
    group by date_format(callback_time,'yyyy-MM-dd'),activity_rule_id,activity_id
)
insert overwrite table dws_activity_info_daycount partition(dt)
select
    activity_rule_id,
    activity_id,
    sum(order_count),
    sum(order_reduce_amount),
    sum(order_original_amount),
    sum(order_final_amount),
    sum(payment_count),
    sum(payment_reduce_amount),
    sum(payment_amount),
    dt
from
(
    select
        dt,
        activity_rule_id,
        activity_id,
        order_count,
        order_reduce_amount,
        order_original_amount,
        order_final_amount,
        0 payment_count,
        0 payment_reduce_amount,
        0 payment_amount
    from tmp_order
    union all
    select
        dt,
        activity_rule_id,
        activity_id,
        0 order_count,
        0 order_reduce_amount,
        0 order_original_amount,
        0 order_final_amount,
        payment_count,
        payment_reduce_amount,
        payment_amount
    from tmp_pay
)t1
group by dt,activity_rule_id,activity_id;

---------------------------------------dws_coupon_info_daycount----------------------------------
DROP TABLE IF EXISTS dws_area_stats_daycount;
CREATE EXTERNAL TABLE dws_area_stats_daycount(
    `province_id` STRING COMMENT '省份编号',
    `visit_count` BIGINT COMMENT '访客访问次数',
    `login_count` BIGINT COMMENT '用户访问次数',
    `visitor_count` BIGINT COMMENT '访客人数',
    `user_count` BIGINT COMMENT '用户人数',
    `order_count` BIGINT COMMENT '下单次数',
    `order_original_amount` DECIMAL(16,2) COMMENT '下单原始金额',
    `order_final_amount` DECIMAL(16,2) COMMENT '下单最终金额',
    `payment_count` BIGINT COMMENT '支付次数',
    `payment_amount` DECIMAL(16,2) COMMENT '支付金额',
    `refund_order_count` BIGINT COMMENT '退单次数',
    `refund_order_amount` DECIMAL(16,2) COMMENT '退单金额',
    `refund_payment_count` BIGINT COMMENT '退款次数',
    `refund_payment_amount` DECIMAL(16,2) COMMENT '退款金额'
) COMMENT '每日地区统计表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall/dws/dws_area_stats_daycount/'
TBLPROPERTIES ("parquet.compression"="lzo");

with
tmp_vu as
(
    select
        dt,
        id province_id,
        visit_count,
        login_count,
        visitor_count,
        user_count
    from
    (
        select
            dt,
            area_code,
            count(*) visit_count,--访客访问次数
            count(user_id) login_count,--用户访问次数,等价于sum(if(user_id is not null,1,0))
            count(distinct(mid_id)) visitor_count,--访客人数
            count(distinct(user_id)) user_count--用户人数
        from dwd_page_log
        where last_page_id is null
        group by dt,area_code
    )tmp
    left join dim_base_province area
    on tmp.area_code=area.area_code
),
tmp_order as
(
    select
        date_format(create_time,'yyyy-MM-dd') dt,
        province_id,
        count(*) order_count,
        sum(original_amount) order_original_amount,
        sum(final_amount) order_final_amount
    from dwd_order_info
    group by date_format(create_time,'yyyy-MM-dd'),province_id
),
tmp_pay as
(
    select
        date_format(callback_time,'yyyy-MM-dd') dt,
        province_id,
        count(*) payment_count,
        sum(payment_amount) payment_amount
    from dwd_payment_info
    group by date_format(callback_time,'yyyy-MM-dd'),province_id
),
tmp_ro as
(
    select
        date_format(create_time,'yyyy-MM-dd') dt,
        province_id,
        count(*) refund_order_count,
        sum(refund_amount) refund_order_amount
    from dwd_order_refund_info
    group by date_format(create_time,'yyyy-MM-dd'),province_id
),
tmp_rp as
(
    select
        date_format(callback_time,'yyyy-MM-dd') dt,
        province_id,
        count(*) refund_payment_count,
        sum(refund_amount) refund_payment_amount
    from dwd_refund_payment
    group by date_format(callback_time,'yyyy-MM-dd'),province_id
)
insert overwrite table dws_area_stats_daycount partition(dt)
select
    province_id,
    sum(visit_count),
    sum(login_count),
    sum(visitor_count),
    sum(user_count),
    sum(order_count),
    sum(order_original_amount),
    sum(order_final_amount),
    sum(payment_count),
    sum(payment_amount),
    sum(refund_order_count),
    sum(refund_order_amount),
    sum(refund_payment_count),
    sum(refund_payment_amount),
    dt
from
(
    select
        dt,
        province_id,
        visit_count,
        login_count,
        visitor_count,
        user_count,
        0 order_count,
        0 order_original_amount,
        0 order_final_amount,
        0 payment_count,
        0 payment_amount,
        0 refund_order_count,
        0 refund_order_amount,
        0 refund_payment_count,
        0 refund_payment_amount
    from tmp_vu
    union all
    select
        dt,
        province_id,
        0 visit_count,
        0 login_count,
        0 visitor_count,
        0 user_count,
        order_count,
        order_original_amount,
        order_final_amount,
        0 payment_count,
        0 payment_amount,
        0 refund_order_count,
        0 refund_order_amount,
        0 refund_payment_count,
        0 refund_payment_amount
    from tmp_order
    union all
    select
        dt,
        province_id,
        0 visit_count,
        0 login_count,
        0 visitor_count,
        0 user_count,
        0 order_count,
        0 order_original_amount,
        0 order_final_amount,
        payment_count,
        payment_amount,
        0 refund_order_count,
        0 refund_order_amount,
        0 refund_payment_count,
        0 refund_payment_amount
    from tmp_pay
    union all
    select
        dt,
        province_id,
        0 visit_count,
        0 login_count,
        0 visitor_count,
        0 user_count,
        0 order_count,
        0 order_original_amount,
        0 order_final_amount,
        0 payment_count,
        0 payment_amount,
        refund_order_count,
        refund_order_amount,
        0 refund_payment_count,
        0 refund_payment_amount
    from tmp_ro
    union all
    select
        dt,
        province_id,
        0 visit_count,
        0 login_count,
        0 visitor_count,
        0 user_count,
        0 order_count,
        0 order_original_amount,
        0 order_final_amount,
        0 payment_count,
        0 payment_amount,
        0 refund_order_count,
        0 refund_order_amount,
        refund_payment_count,
        refund_payment_amount
    from tmp_rp
)t1
group by dt,province_id;

---------------------------------------dwt_visitor_topic----------------------------------
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

---------------------------------------dwt_user_topic----------------------------------
CREATE EXTERNAL TABLE dwt_user_topic
(
    `user_id` STRING  COMMENT '用户id',
    `login_date_first` STRING COMMENT '首次活跃日期',
    `login_date_last` STRING COMMENT '末次活跃日期',
    `login_date_1d_count` STRING COMMENT '最近1日登录次数',
    `login_last_1d_day_count` BIGINT COMMENT '最近1日登录天数',
    `login_last_7d_count` BIGINT COMMENT '最近7日登录次数',
    `login_last_7d_day_count` BIGINT COMMENT '最近7日登录天数',
    `login_last_30d_count` BIGINT COMMENT '最近30日登录次数',
    `login_last_30d_day_count` BIGINT COMMENT '最近30日登录天数',
    `login_count` BIGINT COMMENT '累积登录次数',
    `login_day_count` BIGINT COMMENT '累积登录天数',
    `order_date_first` STRING COMMENT '首次下单时间',
    `order_date_last` STRING COMMENT '末次下单时间',
    `order_last_1d_count` BIGINT COMMENT '最近1日下单次数',
    `order_activity_last_1d_count` BIGINT COMMENT '最近1日订单参与活动次数',
    `order_activity_reduce_last_1d_amount` DECIMAL(16,2) COMMENT '最近1日订单减免金额(活动)',
    `order_coupon_last_1d_count` BIGINT COMMENT '最近1日下单用券次数',
    `order_coupon_reduce_last_1d_amount` DECIMAL(16,2) COMMENT '最近1日订单减免金额(优惠券)',
    `order_last_1d_original_amount` DECIMAL(16,2) COMMENT '最近1日原始下单金额',
    `order_last_1d_final_amount` DECIMAL(16,2) COMMENT '最近1日最终下单金额',
    `order_last_7d_count` BIGINT COMMENT '最近7日下单次数',
    `order_activity_last_7d_count` BIGINT COMMENT '最近7日订单参与活动次数',
    `order_activity_reduce_last_7d_amount` DECIMAL(16,2) COMMENT '最近7日订单减免金额(活动)',
    `order_coupon_last_7d_count` BIGINT COMMENT '最近7日下单用券次数',
    `order_coupon_reduce_last_7d_amount` DECIMAL(16,2) COMMENT '最近7日订单减免金额(优惠券)',
    `order_last_7d_original_amount` DECIMAL(16,2) COMMENT '最近7日原始下单金额',
    `order_last_7d_final_amount` DECIMAL(16,2) COMMENT '最近7日最终下单金额',
    `order_last_30d_count` BIGINT COMMENT '最近30日下单次数',
    `order_activity_last_30d_count` BIGINT COMMENT '最近30日订单参与活动次数',
    `order_activity_reduce_last_30d_amount` DECIMAL(16,2) COMMENT '最近30日订单减免金额(活动)',
    `order_coupon_last_30d_count` BIGINT COMMENT '最近30日下单用券次数',
    `order_coupon_reduce_last_30d_amount` DECIMAL(16,2) COMMENT '最近30日订单减免金额(优惠券)',
    `order_last_30d_original_amount` DECIMAL(16,2) COMMENT '最近30日原始下单金额',
    `order_last_30d_final_amount` DECIMAL(16,2) COMMENT '最近30日最终下单金额',
    `order_count` BIGINT COMMENT '累积下单次数',
    `order_activity_count` BIGINT COMMENT '累积订单参与活动次数',
    `order_activity_reduce_amount` DECIMAL(16,2) COMMENT '累积订单减免金额(活动)',
    `order_coupon_count` BIGINT COMMENT '累积下单用券次数',
    `order_coupon_reduce_amount` DECIMAL(16,2) COMMENT '累积订单减免金额(优惠券)',
    `order_original_amount` DECIMAL(16,2) COMMENT '累积原始下单金额',
    `order_final_amount` DECIMAL(16,2) COMMENT '累积最终下单金额',
    `payment_date_first` STRING COMMENT '首次支付时间',
    `payment_date_last` STRING COMMENT '末次支付时间',
    `payment_last_1d_count` BIGINT COMMENT '最近1日支付次数',
    `payment_last_1d_amount` DECIMAL(16,2) COMMENT '最近1日支付金额',
    `payment_last_7d_count` BIGINT COMMENT '最近7日支付次数',
    `payment_last_7d_amount` DECIMAL(16,2) COMMENT '最近7日支付金额',
    `payment_last_30d_count` BIGINT COMMENT '最近30日支付次数',
    `payment_last_30d_amount` DECIMAL(16,2) COMMENT '最近30日支付金额',
    `payment_count` BIGINT COMMENT '累积支付次数',
    `payment_amount` DECIMAL(16,2) COMMENT '累积支付金额',
    `refund_order_last_1d_count` BIGINT COMMENT '最近1日退单次数',
    `refund_order_last_1d_num` BIGINT COMMENT '最近1日退单件数',
    `refund_order_last_1d_amount` DECIMAL(16,2) COMMENT '最近1日退单金额',
    `refund_order_last_7d_count` BIGINT COMMENT '最近7日退单次数',
    `refund_order_last_7d_num` BIGINT COMMENT '最近7日退单件数',
    `refund_order_last_7d_amount` DECIMAL(16,2) COMMENT '最近7日退单金额',
    `refund_order_last_30d_count` BIGINT COMMENT '最近30日退单次数',
    `refund_order_last_30d_num` BIGINT COMMENT '最近30日退单件数',
    `refund_order_last_30d_amount` DECIMAL(16,2) COMMENT '最近30日退单金额',
    `refund_order_count` BIGINT COMMENT '累积退单次数',
    `refund_order_num` BIGINT COMMENT '累积退单件数',
    `refund_order_amount` DECIMAL(16,2) COMMENT '累积退单金额',
    `refund_payment_last_1d_count` BIGINT COMMENT '最近1日退款次数',
    `refund_payment_last_1d_num` BIGINT COMMENT '最近1日退款件数',
    `refund_payment_last_1d_amount` DECIMAL(16,2) COMMENT '最近1日退款金额',
    `refund_payment_last_7d_count` BIGINT COMMENT '最近7日退款次数',
    `refund_payment_last_7d_num` BIGINT COMMENT '最近7日退款件数',
    `refund_payment_last_7d_amount` DECIMAL(16,2) COMMENT '最近7日退款金额',
    `refund_payment_last_30d_count` BIGINT COMMENT '最近30日退款次数',
    `refund_payment_last_30d_num` BIGINT COMMENT '最近30日退款件数',
    `refund_payment_last_30d_amount` DECIMAL(16,2) COMMENT '最近30日退款金额',
    `refund_payment_count` BIGINT COMMENT '累积退款次数',
    `refund_payment_num` BIGINT COMMENT '累积退款件数',
    `refund_payment_amount` DECIMAL(16,2) COMMENT '累积退款金额',
    `cart_last_1d_count` BIGINT COMMENT '最近1日加入购物车次数',
    `cart_last_7d_count` BIGINT COMMENT '最近7日加入购物车次数',
    `cart_last_30d_count` BIGINT COMMENT '最近30日加入购物车次数',
    `cart_count` BIGINT COMMENT '累积加入购物车次数',
    `favor_last_1d_count` BIGINT COMMENT '最近1日收藏次数',
    `favor_last_7d_count` BIGINT COMMENT '最近7日收藏次数',
    `favor_last_30d_count` BIGINT COMMENT '最近30日收藏次数',
    `favor_count` BIGINT COMMENT '累积收藏次数',
    `coupon_last_1d_get_count` BIGINT COMMENT '最近1日领券次数',
    `coupon_last_1d_using_count` BIGINT COMMENT '最近1日用券(下单)次数',
    `coupon_last_1d_used_count` BIGINT COMMENT '最近1日用券(支付)次数',
    `coupon_last_7d_get_count` BIGINT COMMENT '最近7日领券次数',
    `coupon_last_7d_using_count` BIGINT COMMENT '最近7日用券(下单)次数',
    `coupon_last_7d_used_count` BIGINT COMMENT '最近7日用券(支付)次数',
    `coupon_last_30d_get_count` BIGINT COMMENT '最近30日领券次数',
    `coupon_last_30d_using_count` BIGINT COMMENT '最近30日用券(下单)次数',
    `coupon_last_30d_used_count` BIGINT COMMENT '最近30日用券(支付)次数',
    `coupon_get_count` BIGINT COMMENT '累积领券次数',
    `coupon_using_count` BIGINT COMMENT '累积用券(下单)次数',
    `coupon_used_count` BIGINT COMMENT '累积用券(支付)次数',
    `appraise_last_1d_good_count` BIGINT COMMENT '最近1日好评次数',
    `appraise_last_1d_mid_count` BIGINT COMMENT '最近1日好评次数',
    `appraise_last_1d_bad_count` BIGINT COMMENT '最近1日好评次数',
    `appraise_last_1d_default_count` BIGINT COMMENT '最近1日好评次数',
    `appraise_last_7d_good_count` BIGINT COMMENT '最近7日好评次数',
    `appraise_last_7d_mid_count` BIGINT COMMENT '最近7日好评次数',
    `appraise_last_7d_bad_count` BIGINT COMMENT '最近7日好评次数',
    `appraise_last_7d_default_count` BIGINT COMMENT '最近7日好评次数',
    `appraise_last_30d_good_count` BIGINT COMMENT '最近30日好评次数',
    `appraise_last_30d_mid_count` BIGINT COMMENT '最近30日好评次数',
    `appraise_last_30d_bad_count` BIGINT COMMENT '最近30日好评次数',
    `appraise_last_30d_default_count` BIGINT COMMENT '最近30日好评次数',
    `appraise_good_count` BIGINT COMMENT '累积好评次数',
    `appraise_mid_count` BIGINT COMMENT '累积好评次数',
    `appraise_bad_count` BIGINT COMMENT '累积好评次数',
    `appraise_default_count` BIGINT COMMENT '累积好评次数'
)COMMENT '会员主题宽表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall/dwt/dwt_user_topic/'
TBLPROPERTIES ("parquet.compression"="lzo");

insert overwrite table dwt_user_topic partition(dt='2020-06-14')
select
    id,
    login_date_first,--以用户的创建日期作为首次登录日期
    nvl(login_date_last,date_add('2020-06-14',-1)),--若有历史登录记录，则根据历史记录获取末次登录日期，否则统一指定一个日期
    nvl(login_last_1d_count,0),
    nvl(login_last_1d_day_count,0),
    nvl(login_last_7d_count,0),
    nvl(login_last_7d_day_count,0),
    nvl(login_last_30d_count,0),
    nvl(login_last_30d_day_count,0),
    nvl(login_count,0),
    nvl(login_day_count,0),
    order_date_first,
    order_date_last,
    nvl(order_last_1d_count,0),
    nvl(order_activity_last_1d_count,0),
    nvl(order_activity_reduce_last_1d_amount,0),
    nvl(order_coupon_last_1d_count,0),
    nvl(order_coupon_reduce_last_1d_amount,0),
    nvl(order_last_1d_original_amount,0),
    nvl(order_last_1d_final_amount,0),
    nvl(order_last_7d_count,0),
    nvl(order_activity_last_7d_count,0),
    nvl(order_activity_reduce_last_7d_amount,0),
    nvl(order_coupon_last_7d_count,0),
    nvl(order_coupon_reduce_last_7d_amount,0),
    nvl(order_last_7d_original_amount,0),
    nvl(order_last_7d_final_amount,0),
    nvl(order_last_30d_count,0),
    nvl(order_activity_last_30d_count,0),
    nvl(order_activity_reduce_last_30d_amount,0),
    nvl(order_coupon_last_30d_count,0),
    nvl(order_coupon_reduce_last_30d_amount,0),
    nvl(order_last_30d_original_amount,0),
    nvl(order_last_30d_final_amount,0),
    nvl(order_count,0),
    nvl(order_activity_count,0),
    nvl(order_activity_reduce_amount,0),
    nvl(order_coupon_count,0),
    nvl(order_coupon_reduce_amount,0),
    nvl(order_original_amount,0),
    nvl(order_final_amount,0),
    payment_date_first,
    payment_date_last,
    nvl(payment_last_1d_count,0),
    nvl(payment_last_1d_amount,0),
    nvl(payment_last_7d_count,0),
    nvl(payment_last_7d_amount,0),
    nvl(payment_last_30d_count,0),
    nvl(payment_last_30d_amount,0),
    nvl(payment_count,0),
    nvl(payment_amount,0),
    nvl(refund_order_last_1d_count,0),
    nvl(refund_order_last_1d_num,0),
    nvl(refund_order_last_1d_amount,0),
    nvl(refund_order_last_7d_count,0),
    nvl(refund_order_last_7d_num,0),
    nvl(refund_order_last_7d_amount,0),
    nvl(refund_order_last_30d_count,0),
    nvl(refund_order_last_30d_num,0),
    nvl(refund_order_last_30d_amount,0),
    nvl(refund_order_count,0),
    nvl(refund_order_num,0),
    nvl(refund_order_amount,0),
    nvl(refund_payment_last_1d_count,0),
    nvl(refund_payment_last_1d_num,0),
    nvl(refund_payment_last_1d_amount,0),
    nvl(refund_payment_last_7d_count,0),
    nvl(refund_payment_last_7d_num,0),
    nvl(refund_payment_last_7d_amount,0),
    nvl(refund_payment_last_30d_count,0),
    nvl(refund_payment_last_30d_num,0),
    nvl(refund_payment_last_30d_amount,0),
    nvl(refund_payment_count,0),
    nvl(refund_payment_num,0),
    nvl(refund_payment_amount,0),
    nvl(cart_last_1d_count,0),
    nvl(cart_last_7d_count,0),
    nvl(cart_last_30d_count,0),
    nvl(cart_count,0),
    nvl(favor_last_1d_count,0),
    nvl(favor_last_7d_count,0),
    nvl(favor_last_30d_count,0),
    nvl(favor_count,0),
    nvl(coupon_last_1d_get_count,0),
    nvl(coupon_last_1d_using_count,0),
    nvl(coupon_last_1d_used_count,0),
    nvl(coupon_last_7d_get_count,0),
    nvl(coupon_last_7d_using_count,0),
    nvl(coupon_last_7d_used_count,0),
    nvl(coupon_last_30d_get_count,0),
    nvl(coupon_last_30d_using_count,0),
    nvl(coupon_last_30d_used_count,0),
    nvl(coupon_get_count,0),
    nvl(coupon_using_count,0),
    nvl(coupon_used_count,0),
    nvl(appraise_last_1d_good_count,0),
    nvl(appraise_last_1d_mid_count,0),
    nvl(appraise_last_1d_bad_count,0),
    nvl(appraise_last_1d_default_count,0),
    nvl(appraise_last_7d_good_count,0),
    nvl(appraise_last_7d_mid_count,0),
    nvl(appraise_last_7d_bad_count,0),
    nvl(appraise_last_7d_default_count,0),
    nvl(appraise_last_30d_good_count,0),
    nvl(appraise_last_30d_mid_count,0),
    nvl(appraise_last_30d_bad_count,0),
    nvl(appraise_last_30d_default_count,0),
    nvl(appraise_good_count,0),
    nvl(appraise_mid_count,0),
    nvl(appraise_bad_count,0),
    nvl(appraise_default_count,0)
from
(
    select
        id,
        date_format(create_time,'yyyy-MM-dd') login_date_first
    from dim_user_info
    where dt='9999-99-99'
)t1
left join
(
    select
        user_id user_id,
        max(dt) login_date_last,
        sum(if(dt='2020-06-14',login_count,0)) login_last_1d_count,
        sum(if(dt='2020-06-14' and login_count>0,1,0)) login_last_1d_day_count,
        sum(if(dt>=date_add('2020-06-14',-6),login_count,0)) login_last_7d_count,
        sum(if(dt>=date_add('2020-06-14',-6) and login_count>0,1,0)) login_last_7d_day_count,
        sum(if(dt>=date_add('2020-06-14',-29),login_count,0)) login_last_30d_count,
        sum(if(dt>=date_add('2020-06-14',-29) and login_count>0,1,0)) login_last_30d_day_count,
        sum(login_count) login_count,
        sum(if(login_count>0,1,0)) login_day_count,
        min(if(order_count>0,dt,null)) order_date_first,
        max(if(order_count>0,dt,null)) order_date_last,
        sum(if(dt='2020-06-14',order_count,0)) order_last_1d_count,
        sum(if(dt='2020-06-14',order_activity_count,0)) order_activity_last_1d_count,
        sum(if(dt='2020-06-14',order_activity_reduce_amount,0)) order_activity_reduce_last_1d_amount,
        sum(if(dt='2020-06-14',order_coupon_count,0)) order_coupon_last_1d_count,
        sum(if(dt='2020-06-14',order_coupon_reduce_amount,0)) order_coupon_reduce_last_1d_amount,
        sum(if(dt='2020-06-14',order_original_amount,0)) order_last_1d_original_amount,
        sum(if(dt='2020-06-14',order_final_amount,0)) order_last_1d_final_amount,
        sum(if(dt>=date_add('2020-06-14',-6),order_count,0)) order_last_7d_count,
        sum(if(dt>=date_add('2020-06-14',-6),order_activity_count,0)) order_activity_last_7d_count,
        sum(if(dt>=date_add('2020-06-14',-6),order_activity_reduce_amount,0)) order_activity_reduce_last_7d_amount,
        sum(if(dt>=date_add('2020-06-14',-6),order_coupon_count,0)) order_coupon_last_7d_count,
        sum(if(dt>=date_add('2020-06-14',-6),order_coupon_reduce_amount,0)) order_coupon_reduce_last_7d_amount,
        sum(if(dt>=date_add('2020-06-14',-6),order_original_amount,0)) order_last_7d_original_amount,
        sum(if(dt>=date_add('2020-06-14',-6),order_final_amount,0)) order_last_7d_final_amount,
        sum(if(dt>=date_add('2020-06-14',-29),order_count,0)) order_last_30d_count,
        sum(if(dt>=date_add('2020-06-14',-29),order_activity_count,0)) order_activity_last_30d_count,
        sum(if(dt>=date_add('2020-06-14',-29),order_activity_reduce_amount,0)) order_activity_reduce_last_30d_amount,
        sum(if(dt>=date_add('2020-06-14',-29),order_coupon_count,0)) order_coupon_last_30d_count,
        sum(if(dt>=date_add('2020-06-14',-29),order_coupon_reduce_amount,0)) order_coupon_reduce_last_30d_amount,
        sum(if(dt>=date_add('2020-06-14',-29),order_original_amount,0)) order_last_30d_original_amount,
        sum(if(dt>=date_add('2020-06-14',-29),order_final_amount,0)) order_last_30d_final_amount,
        sum(order_count) order_count,
        sum(order_activity_count) order_activity_count,
        sum(order_activity_reduce_amount) order_activity_reduce_amount,
        sum(order_coupon_count) order_coupon_count,
        sum(order_coupon_reduce_amount) order_coupon_reduce_amount,
        sum(order_original_amount) order_original_amount,
        sum(order_final_amount) order_final_amount,
        min(if(payment_count>0,dt,null)) payment_date_first,
        max(if(payment_count>0,dt,null)) payment_date_last,
        sum(if(dt='2020-06-14',payment_count,0)) payment_last_1d_count,
        sum(if(dt='2020-06-14',payment_amount,0)) payment_last_1d_amount,
        sum(if(dt>=date_add('2020-06-14',-6),payment_count,0)) payment_last_7d_count,
        sum(if(dt>=date_add('2020-06-14',-6),payment_amount,0)) payment_last_7d_amount,
        sum(if(dt>=date_add('2020-06-14',-29),payment_count,0)) payment_last_30d_count,
        sum(if(dt>=date_add('2020-06-14',-29),payment_amount,0)) payment_last_30d_amount,
        sum(payment_count) payment_count,
        sum(payment_amount) payment_amount,
        sum(if(dt='2020-06-14',refund_order_count,0)) refund_order_last_1d_count,
        sum(if(dt='2020-06-14',refund_order_num,0)) refund_order_last_1d_num,
        sum(if(dt='2020-06-14',refund_order_amount,0)) refund_order_last_1d_amount,
        sum(if(dt>=date_add('2020-06-14',-6),refund_order_count,0)) refund_order_last_7d_count,
        sum(if(dt>=date_add('2020-06-14',-6),refund_order_num,0)) refund_order_last_7d_num,
        sum(if(dt>=date_add('2020-06-14',-6),refund_order_amount,0)) refund_order_last_7d_amount,
        sum(if(dt>=date_add('2020-06-14',-29),refund_order_count,0)) refund_order_last_30d_count,
        sum(if(dt>=date_add('2020-06-14',-29),refund_order_num,0)) refund_order_last_30d_num,
        sum(if(dt>=date_add('2020-06-14',-29),refund_order_amount,0)) refund_order_last_30d_amount,
        sum(refund_order_count) refund_order_count,
        sum(refund_order_num) refund_order_num,
        sum(refund_order_amount) refund_order_amount,
        sum(if(dt='2020-06-14',refund_payment_count,0)) refund_payment_last_1d_count,
        sum(if(dt='2020-06-14',refund_payment_num,0)) refund_payment_last_1d_num,
        sum(if(dt='2020-06-14',refund_payment_amount,0)) refund_payment_last_1d_amount,
        sum(if(dt>=date_add('2020-06-14',-6),refund_payment_count,0)) refund_payment_last_7d_count,
        sum(if(dt>=date_add('2020-06-14',-6),refund_payment_num,0)) refund_payment_last_7d_num,
        sum(if(dt>=date_add('2020-06-14',-6),refund_payment_amount,0)) refund_payment_last_7d_amount,
        sum(if(dt>=date_add('2020-06-14',-29),refund_payment_count,0)) refund_payment_last_30d_count,
        sum(if(dt>=date_add('2020-06-14',-29),refund_payment_num,0)) refund_payment_last_30d_num,
        sum(if(dt>=date_add('2020-06-14',-29),refund_payment_amount,0)) refund_payment_last_30d_amount,
        sum(refund_payment_count) refund_payment_count,
        sum(refund_payment_num) refund_payment_num,
        sum(refund_payment_amount) refund_payment_amount,
        sum(if(dt='2020-06-14',cart_count,0)) cart_last_1d_count,
        sum(if(dt>=date_add('2020-06-14',-6),cart_count,0)) cart_last_7d_count,
        sum(if(dt>=date_add('2020-06-14',-29),cart_count,0)) cart_last_30d_count,
        sum(cart_count) cart_count,
        sum(if(dt='2020-06-14',favor_count,0)) favor_last_1d_count,
        sum(if(dt>=date_add('2020-06-14',-6),favor_count,0)) favor_last_7d_count,
        sum(if(dt>=date_add('2020-06-14',-29),favor_count,0)) favor_last_30d_count,
        sum(favor_count) favor_count,
        sum(if(dt='2020-06-14',coupon_get_count,0)) coupon_last_1d_get_count,
        sum(if(dt='2020-06-14',coupon_using_count,0)) coupon_last_1d_using_count,
        sum(if(dt='2020-06-14',coupon_used_count,0)) coupon_last_1d_used_count,
        sum(if(dt>=date_add('2020-06-14',-6),coupon_get_count,0)) coupon_last_7d_get_count,
        sum(if(dt>=date_add('2020-06-14',-6),coupon_using_count,0)) coupon_last_7d_using_count,
        sum(if(dt>=date_add('2020-06-14',-6),coupon_used_count,0)) coupon_last_7d_used_count,
        sum(if(dt>=date_add('2020-06-14',-29),coupon_get_count,0)) coupon_last_30d_get_count,
        sum(if(dt>=date_add('2020-06-14',-29),coupon_using_count,0)) coupon_last_30d_using_count,
        sum(if(dt>=date_add('2020-06-14',-29),coupon_used_count,0)) coupon_last_30d_used_count,
        sum(coupon_get_count) coupon_get_count,
        sum(coupon_using_count) coupon_using_count,
        sum(coupon_used_count) coupon_used_count,
        sum(if(dt='2020-06-14',appraise_good_count,0)) appraise_last_1d_good_count,
        sum(if(dt='2020-06-14',appraise_mid_count,0)) appraise_last_1d_mid_count,
        sum(if(dt='2020-06-14',appraise_bad_count,0)) appraise_last_1d_bad_count,
        sum(if(dt='2020-06-14',appraise_default_count,0)) appraise_last_1d_default_count,
        sum(if(dt>=date_add('2020-06-14',-6),appraise_good_count,0)) appraise_last_7d_good_count,
        sum(if(dt>=date_add('2020-06-14',-6),appraise_mid_count,0)) appraise_last_7d_mid_count,
        sum(if(dt>=date_add('2020-06-14',-6),appraise_bad_count,0)) appraise_last_7d_bad_count,
        sum(if(dt>=date_add('2020-06-14',-6),appraise_default_count,0)) appraise_last_7d_default_count,
        sum(if(dt>=date_add('2020-06-14',-29),appraise_good_count,0)) appraise_last_30d_good_count,
        sum(if(dt>=date_add('2020-06-14',-29),appraise_mid_count,0)) appraise_last_30d_mid_count,
        sum(if(dt>=date_add('2020-06-14',-29),appraise_bad_count,0)) appraise_last_30d_bad_count,
        sum(if(dt>=date_add('2020-06-14',-29),appraise_default_count,0)) appraise_last_30d_default_count,
        sum(appraise_good_count) appraise_good_count,
        sum(appraise_mid_count) appraise_mid_count,
        sum(appraise_bad_count) appraise_bad_count,
        sum(appraise_default_count) appraise_default_count
    from dws_user_action_daycount
    group by user_id
)t2
on t1.id=t2.user_id;

insert overwrite table dwt_user_topic partition(dt='2020-06-15')
select
    nvl(1d_ago.user_id,old.user_id),
    nvl(old.login_date_first,'2020-06-15'),
    if(1d_ago.user_id is not null,'2020-06-15',old.login_date_last),
    nvl(1d_ago.login_count,0),
    if(1d_ago.user_id is not null,1,0),
    nvl(old.login_last_7d_count,0)+nvl(1d_ago.login_count,0)- nvl(7d_ago.login_count,0),
    nvl(old.login_last_7d_day_count,0)+if(1d_ago.user_id is null,0,1)- if(7d_ago.user_id is null,0,1),
    nvl(old.login_last_30d_count,0)+nvl(1d_ago.login_count,0)- nvl(30d_ago.login_count,0),
    nvl(old.login_last_30d_day_count,0)+if(1d_ago.user_id is null,0,1)- if(30d_ago.user_id is null,0,1),
    nvl(old.login_count,0)+nvl(1d_ago.login_count,0),
    nvl(old.login_day_count,0)+if(1d_ago.user_id is not null,1,0),
    if(old.order_date_first is null and 1d_ago.order_count>0, '2020-06-15', old.order_date_first),
    if(1d_ago.order_count>0,'2020-06-15',old.order_date_last),
    nvl(1d_ago.order_count,0),
    nvl(1d_ago.order_activity_count,0),
    nvl(1d_ago.order_activity_reduce_amount,0.0),
    nvl(1d_ago.order_coupon_count,0),
    nvl(1d_ago.order_coupon_reduce_amount,0.0),
    nvl(1d_ago.order_original_amount,0.0),
    nvl(1d_ago.order_final_amount,0.0),
    nvl(old.order_last_7d_count,0)+nvl(1d_ago.order_count,0)- nvl(7d_ago.order_count,0),
    nvl(old.order_activity_last_7d_count,0)+nvl(1d_ago.order_activity_count,0)- nvl(7d_ago.order_activity_count,0),
    nvl(old.order_activity_reduce_last_7d_amount,0.0)+nvl(1d_ago.order_activity_reduce_amount,0.0)- nvl(7d_ago.order_activity_reduce_amount,0.0),
    nvl(old.order_coupon_last_7d_count,0)+nvl(1d_ago.order_coupon_count,0)- nvl(7d_ago.order_coupon_count,0),
    nvl(old.order_coupon_reduce_last_7d_amount,0.0)+nvl(1d_ago.order_coupon_reduce_amount,0.0)- nvl(7d_ago.order_coupon_reduce_amount,0.0),
    nvl(old.order_last_7d_original_amount,0.0)+nvl(1d_ago.order_original_amount,0.0)- nvl(7d_ago.order_original_amount,0.0),
    nvl(old.order_last_7d_final_amount,0.0)+nvl(1d_ago.order_final_amount,0.0)- nvl(7d_ago.order_final_amount,0.0),
    nvl(old.order_last_30d_count,0)+nvl(1d_ago.order_count,0)- nvl(30d_ago.order_count,0),
    nvl(old.order_activity_last_30d_count,0)+nvl(1d_ago.order_activity_count,0)- nvl(30d_ago.order_activity_count,0),
    nvl(old.order_activity_reduce_last_30d_amount,0.0)+nvl(1d_ago.order_activity_reduce_amount,0.0)- nvl(30d_ago.order_activity_reduce_amount,0.0),
    nvl(old.order_coupon_last_30d_count,0)+nvl(1d_ago.order_coupon_count,0)- nvl(30d_ago.order_coupon_count,0),
    nvl(old.order_coupon_reduce_last_30d_amount,0.0)+nvl(1d_ago.order_coupon_reduce_amount,0.0)- nvl(30d_ago.order_coupon_reduce_amount,0.0),
    nvl(old.order_last_30d_original_amount,0.0)+nvl(1d_ago.order_original_amount,0.0)- nvl(30d_ago.order_original_amount,0.0),
    nvl(old.order_last_30d_final_amount,0.0)+nvl(1d_ago.order_final_amount,0.0)- nvl(30d_ago.order_final_amount,0.0),
    nvl(old.order_count,0)+nvl(1d_ago.order_count,0),
    nvl(old.order_activity_count,0)+nvl(1d_ago.order_activity_count,0),
    nvl(old.order_activity_reduce_amount,0.0)+nvl(1d_ago.order_activity_reduce_amount,0.0),
    nvl(old.order_coupon_count,0)+nvl(1d_ago.order_coupon_count,0),
    nvl(old.order_coupon_reduce_amount,0.0)+nvl(1d_ago.order_coupon_reduce_amount,0.0),
    nvl(old.order_original_amount,0.0)+nvl(1d_ago.order_original_amount,0.0),
    nvl(old.order_final_amount,0.0)+nvl(1d_ago.order_final_amount,0.0),
    if(old.payment_date_first is null and 1d_ago.payment_count>0, '2020-06-15', old.payment_date_first),
    if(1d_ago.payment_count>0,'2020-06-15',old.payment_date_last),
    nvl(1d_ago.payment_count,0),
    nvl(1d_ago.payment_amount,0.0),
    nvl(old.payment_last_7d_count,0)+nvl(1d_ago.payment_count,0)-nvl(7d_ago.payment_count,0),
    nvl(old.payment_last_7d_amount,0.0)+nvl(1d_ago.payment_amount,0.0)-nvl(7d_ago.payment_amount,0.0),
    nvl(old.payment_last_30d_count,0)+nvl(1d_ago.payment_count,0)-nvl(30d_ago.payment_count,0),
    nvl(old.payment_last_30d_amount,0.0)+nvl(1d_ago.payment_amount,0.0)- nvl(30d_ago.payment_amount,0.0),
    nvl(old.payment_count,0)+nvl(1d_ago.payment_count,0),
    nvl(old.payment_amount,0.0)+nvl(1d_ago.payment_amount,0.0),
    nvl(1d_ago.refund_order_count,0),
    nvl(1d_ago.refund_order_num,0),
    nvl(1d_ago.refund_order_amount,0.0),
    nvl(old.refund_order_last_7d_count,0)+nvl(1d_ago.refund_order_count,0)- nvl(7d_ago.refund_order_count,0),
    nvl(old.refund_order_last_7d_num,0)+nvl(1d_ago.refund_order_num, 0)- nvl(7d_ago.refund_order_num,0),
    nvl(old.refund_order_last_7d_amount,0.0)+ nvl(1d_ago.refund_order_amount,0.0)- nvl(7d_ago.refund_order_amount,0.0),
    nvl(old.refund_order_last_30d_count,0)+nvl(1d_ago.refund_order_count,0)- nvl(30d_ago.refund_order_count,0),
    nvl(old.refund_order_last_30d_num,0)+nvl(1d_ago.refund_order_num, 0)- nvl(30d_ago.refund_order_num,0),
    nvl(old.refund_order_last_30d_amount,0.0)+ nvl(1d_ago.refund_order_amount,0.0)- nvl(30d_ago.refund_order_amount,0.0),
    nvl(old.refund_order_count,0)+nvl(1d_ago.refund_order_count,0),
    nvl(old.refund_order_num,0)+nvl(1d_ago.refund_order_num,0),
    nvl(old.refund_order_amount,0.0)+ nvl(1d_ago.refund_order_amount,0.0),
    nvl(1d_ago.refund_payment_count,0),
    nvl(1d_ago.refund_payment_num,0),
    nvl(1d_ago.refund_payment_amount,0.0),
    nvl(old.refund_payment_last_7d_count,0)+nvl(1d_ago.refund_payment_count,0)-nvl(7d_ago.refund_payment_count,0),
    nvl(old.refund_payment_last_7d_num,0)+nvl(1d_ago.refund_payment_num,0)- nvl(7d_ago.refund_payment_num,0),
    nvl(old.refund_payment_last_7d_amount,0.0)+ nvl(1d_ago.refund_payment_amount,0.0)- nvl(7d_ago.refund_payment_amount,0.0),
    nvl(old.refund_payment_last_30d_count,0)+nvl(1d_ago.refund_payment_count,0)-nvl(30d_ago.refund_payment_count,0),
    nvl(old.refund_payment_last_30d_num,0)+nvl(1d_ago.refund_payment_num,0)- nvl(30d_ago.refund_payment_num,0),
    nvl(old.refund_payment_last_30d_amount,0.0)+ nvl(1d_ago.refund_payment_amount,0.0)- nvl(30d_ago.refund_payment_amount,0.0),
    nvl(old.refund_payment_count,0)+nvl(1d_ago.refund_payment_count,0),
    nvl(old.refund_payment_num,0)+nvl(1d_ago.refund_payment_num,0),
    nvl(old.refund_payment_amount,0.0)+nvl(1d_ago.refund_payment_amount,0.0),
    nvl(1d_ago.cart_count,0),
    nvl(old.cart_last_7d_count,0)+nvl(1d_ago.cart_count,0)-nvl(7d_ago.cart_count,0),
    nvl(old.cart_last_30d_count,0)+nvl(1d_ago.cart_count,0)-nvl(30d_ago.cart_count,0),
    nvl(old.cart_count,0)+nvl(1d_ago.cart_count,0),
    nvl(1d_ago.favor_count,0),
    nvl(old.favor_last_7d_count,0)+nvl(1d_ago.favor_count,0)- nvl(7d_ago.favor_count,0),
    nvl(old.favor_last_30d_count,0)+nvl(1d_ago.favor_count,0)- nvl(30d_ago.favor_count,0),
    nvl(old.favor_count,0)+nvl(1d_ago.favor_count,0),
    nvl(1d_ago.coupon_get_count,0),
    nvl(1d_ago.coupon_using_count,0),
    nvl(1d_ago.coupon_used_count,0),
    nvl(old.coupon_last_7d_get_count,0)+nvl(1d_ago.coupon_get_count,0)- nvl(7d_ago.coupon_get_count,0),
    nvl(old.coupon_last_7d_using_count,0)+nvl(1d_ago.coupon_using_count,0)- nvl(7d_ago.coupon_using_count,0),
    nvl(old.coupon_last_7d_used_count,0)+ nvl(1d_ago.coupon_used_count,0)- nvl(7d_ago.coupon_used_count,0),
    nvl(old.coupon_last_30d_get_count,0)+nvl(1d_ago.coupon_get_count,0)- nvl(30d_ago.coupon_get_count,0),
    nvl(old.coupon_last_30d_using_count,0)+nvl(1d_ago.coupon_using_count,0)- nvl(30d_ago.coupon_using_count,0),
    nvl(old.coupon_last_30d_used_count,0)+ nvl(1d_ago.coupon_used_count,0)- nvl(30d_ago.coupon_used_count,0),
    nvl(old.coupon_get_count,0)+nvl(1d_ago.coupon_get_count,0),
    nvl(old.coupon_using_count,0)+nvl(1d_ago.coupon_using_count,0),
    nvl(old.coupon_used_count,0)+nvl(1d_ago.coupon_used_count,0),
    nvl(1d_ago.appraise_good_count,0),
    nvl(1d_ago.appraise_mid_count,0),
    nvl(1d_ago.appraise_bad_count,0),
    nvl(1d_ago.appraise_default_count,0),
    nvl(old.appraise_last_7d_good_count,0)+nvl(1d_ago.appraise_good_count,0)- nvl(7d_ago.appraise_good_count,0),
    nvl(old.appraise_last_7d_mid_count,0)+nvl(1d_ago.appraise_mid_count,0)-nvl(7d_ago.appraise_mid_count,0),
    nvl(old.appraise_last_7d_bad_count,0)+nvl(1d_ago.appraise_bad_count,0)-nvl(7d_ago.appraise_bad_count,0),
    nvl(old.appraise_last_7d_default_count,0)+nvl(1d_ago.appraise_default_count,0)-nvl(7d_ago.appraise_default_count,0),
    nvl(old.appraise_last_30d_good_count,0)+nvl(1d_ago.appraise_good_count,0)- nvl(30d_ago.appraise_good_count,0),
    nvl(old.appraise_last_30d_mid_count,0)+nvl(1d_ago.appraise_mid_count,0)-nvl(30d_ago.appraise_mid_count,0),
    nvl(old.appraise_last_30d_bad_count,0)+nvl(1d_ago.appraise_bad_count,0)-nvl(30d_ago.appraise_bad_count,0),
    nvl(old.appraise_last_30d_default_count,0)+nvl(1d_ago.appraise_default_count,0)-nvl(30d_ago.appraise_default_count,0),
    nvl(old.appraise_good_count,0)+nvl(1d_ago.appraise_good_count,0),
    nvl(old.appraise_mid_count,0)+nvl(1d_ago.appraise_mid_count, 0),
    nvl(old.appraise_bad_count,0)+nvl(1d_ago.appraise_bad_count,0),
    nvl(old.appraise_default_count,0)+nvl(1d_ago.appraise_default_count,0)
from
(
    select
        user_id,
        login_date_first,
        login_date_last,
        login_date_1d_count,
        login_last_1d_day_count,
        login_last_7d_count,
        login_last_7d_day_count,
        login_last_30d_count,
        login_last_30d_day_count,
        login_count,
        login_day_count,
        order_date_first,
        order_date_last,
        order_last_1d_count,
        order_activity_last_1d_count,
        order_activity_reduce_last_1d_amount,
        order_coupon_last_1d_count,
        order_coupon_reduce_last_1d_amount,
        order_last_1d_original_amount,
        order_last_1d_final_amount,
        order_last_7d_count,
        order_activity_last_7d_count,
        order_activity_reduce_last_7d_amount,
        order_coupon_last_7d_count,
        order_coupon_reduce_last_7d_amount,
        order_last_7d_original_amount,
        order_last_7d_final_amount,
        order_last_30d_count,
        order_activity_last_30d_count,
        order_activity_reduce_last_30d_amount,
        order_coupon_last_30d_count,
        order_coupon_reduce_last_30d_amount,
        order_last_30d_original_amount,
        order_last_30d_final_amount,
        order_count,
        order_activity_count,
        order_activity_reduce_amount,
        order_coupon_count,
        order_coupon_reduce_amount,
        order_original_amount,
        order_final_amount,
        payment_date_first,
        payment_date_last,
        payment_last_1d_count,
        payment_last_1d_amount,
        payment_last_7d_count,
        payment_last_7d_amount,
        payment_last_30d_count,
        payment_last_30d_amount,
        payment_count,
        payment_amount,
        refund_order_last_1d_count,
        refund_order_last_1d_num,
        refund_order_last_1d_amount,
        refund_order_last_7d_count,
        refund_order_last_7d_num,
        refund_order_last_7d_amount,
        refund_order_last_30d_count,
        refund_order_last_30d_num,
        refund_order_last_30d_amount,
        refund_order_count,
        refund_order_num,
        refund_order_amount,
        refund_payment_last_1d_count,
        refund_payment_last_1d_num,
        refund_payment_last_1d_amount,
        refund_payment_last_7d_count,
        refund_payment_last_7d_num,
        refund_payment_last_7d_amount,
        refund_payment_last_30d_count,
        refund_payment_last_30d_num,
        refund_payment_last_30d_amount,
        refund_payment_count,
        refund_payment_num,
        refund_payment_amount,
        cart_last_1d_count,
        cart_last_7d_count,
        cart_last_30d_count,
        cart_count,
        favor_last_1d_count,
        favor_last_7d_count,
        favor_last_30d_count,
        favor_count,
        coupon_last_1d_get_count,
        coupon_last_1d_using_count,
        coupon_last_1d_used_count,
        coupon_last_7d_get_count,
        coupon_last_7d_using_count,
        coupon_last_7d_used_count,
        coupon_last_30d_get_count,
        coupon_last_30d_using_count,
        coupon_last_30d_used_count,
        coupon_get_count,
        coupon_using_count,
        coupon_used_count,
        appraise_last_1d_good_count,
        appraise_last_1d_mid_count,
        appraise_last_1d_bad_count,
        appraise_last_1d_default_count,
        appraise_last_7d_good_count,
        appraise_last_7d_mid_count,
        appraise_last_7d_bad_count,
        appraise_last_7d_default_count,
        appraise_last_30d_good_count,
        appraise_last_30d_mid_count,
        appraise_last_30d_bad_count,
        appraise_last_30d_default_count,
        appraise_good_count,
        appraise_mid_count,
        appraise_bad_count,
        appraise_default_count
    from dwt_user_topic
    where dt=date_add('2020-06-15',-1)
)old
full outer join
(
    select
        user_id,
        login_count,
        cart_count,
        favor_count,
        order_count,
        order_activity_count,
        order_activity_reduce_amount,
        order_coupon_count,
        order_coupon_reduce_amount,
        order_original_amount,
        order_final_amount,
        payment_count,
        payment_amount,
        refund_order_count,
        refund_order_num,
        refund_order_amount,
        refund_payment_count,
        refund_payment_num,
        refund_payment_amount,
        coupon_get_count,
        coupon_using_count,
        coupon_used_count,
        appraise_good_count,
        appraise_mid_count,
        appraise_bad_count,
        appraise_default_count
    from dws_user_action_daycount
    where dt='2020-06-15'
)1d_ago
on old.user_id=1d_ago.user_id
left join
(
    select
        user_id,
        login_count,
        cart_count,
        favor_count,
        order_count,
        order_activity_count,
        order_activity_reduce_amount,
        order_coupon_count,
        order_coupon_reduce_amount,
        order_original_amount,
        order_final_amount,
        payment_count,
        payment_amount,
        refund_order_count,
        refund_order_num,
        refund_order_amount,
        refund_payment_count,
        refund_payment_num,
        refund_payment_amount,
        coupon_get_count,
        coupon_using_count,
        coupon_used_count,
        appraise_good_count,
        appraise_mid_count,
        appraise_bad_count,
        appraise_default_count
    from dws_user_action_daycount
    where dt=date_add('2020-06-15',-7)
)7d_ago
on old.user_id=7d_ago.user_id
left join
(
    select
        user_id,
        login_count,
        cart_count,
        favor_count,
        order_count,
        order_activity_count,
        order_activity_reduce_amount,
        order_coupon_count,
        order_coupon_reduce_amount,
        order_original_amount,
        order_final_amount,
        payment_count,
        payment_amount,
        refund_order_count,
        refund_order_num,
        refund_order_amount,
        refund_payment_count,
        refund_payment_num,
        refund_payment_amount,
        coupon_get_count,
        coupon_using_count,
        coupon_used_count,
        appraise_good_count,
        appraise_mid_count,
        appraise_bad_count,
        appraise_default_count
    from dws_user_action_daycount
    where dt=date_add('2020-06-15',-30)
)30d_ago
on old.user_id=30d_ago.user_id;

---------------------------------------dwt_sku_topic----------------------------------
DROP TABLE IF EXISTS dwt_sku_topic;
CREATE EXTERNAL TABLE dwt_sku_topic
(
    `sku_id` STRING COMMENT 'sku_id',
    `order_last_1d_count` BIGINT COMMENT '最近1日被下单次数',
    `order_last_1d_num` BIGINT COMMENT '最近1日被下单件数',
    `order_activity_last_1d_count` BIGINT COMMENT '最近1日参与活动被下单次数',
    `order_coupon_last_1d_count` BIGINT COMMENT '最近1日使用优惠券被下单次数',
    `order_activity_reduce_last_1d_amount` DECIMAL(16,2) COMMENT '最近1日优惠金额(活动)',
    `order_coupon_reduce_last_1d_amount` DECIMAL(16,2) COMMENT '最近1日优惠金额(优惠券)',
    `order_last_1d_original_amount` DECIMAL(16,2) COMMENT '最近1日被下单原始金额',
    `order_last_1d_final_amount` DECIMAL(16,2) COMMENT '最近1日被下单最终金额',
    `order_last_7d_count` BIGINT COMMENT '最近7日被下单次数',
    `order_last_7d_num` BIGINT COMMENT '最近7日被下单件数',
    `order_activity_last_7d_count` BIGINT COMMENT '最近7日参与活动被下单次数',
    `order_coupon_last_7d_count` BIGINT COMMENT '最近7日使用优惠券被下单次数',
    `order_activity_reduce_last_7d_amount` DECIMAL(16,2) COMMENT '最近7日优惠金额(活动)',
    `order_coupon_reduce_last_7d_amount` DECIMAL(16,2) COMMENT '最近7日优惠金额(优惠券)',
    `order_last_7d_original_amount` DECIMAL(16,2) COMMENT '最近7日被下单原始金额',
    `order_last_7d_final_amount` DECIMAL(16,2) COMMENT '最近7日被下单最终金额',
    `order_last_30d_count` BIGINT COMMENT '最近30日被下单次数',
    `order_last_30d_num` BIGINT COMMENT '最近30日被下单件数',
    `order_activity_last_30d_count` BIGINT COMMENT '最近30日参与活动被下单次数',
    `order_coupon_last_30d_count` BIGINT COMMENT '最近30日使用优惠券被下单次数',
    `order_activity_reduce_last_30d_amount` DECIMAL(16,2) COMMENT '最近30日优惠金额(活动)',
    `order_coupon_reduce_last_30d_amount` DECIMAL(16,2) COMMENT '最近30日优惠金额(优惠券)',
    `order_last_30d_original_amount` DECIMAL(16,2) COMMENT '最近30日被下单原始金额',
    `order_last_30d_final_amount` DECIMAL(16,2) COMMENT '最近30日被下单最终金额',
    `order_count` BIGINT COMMENT '累积被下单次数',
    `order_num` BIGINT COMMENT '累积被下单件数',
    `order_activity_count` BIGINT COMMENT '累积参与活动被下单次数',
    `order_coupon_count` BIGINT COMMENT '累积使用优惠券被下单次数',
    `order_activity_reduce_amount` DECIMAL(16,2) COMMENT '累积优惠金额(活动)',
    `order_coupon_reduce_amount` DECIMAL(16,2) COMMENT '累积优惠金额(优惠券)',
    `order_original_amount` DECIMAL(16,2) COMMENT '累积被下单原始金额',
    `order_final_amount` DECIMAL(16,2) COMMENT '累积被下单最终金额',
    `payment_last_1d_count` BIGINT COMMENT '最近1日被支付次数',
    `payment_last_1d_num` BIGINT COMMENT '最近1日被支付件数',
    `payment_last_1d_amount` DECIMAL(16,2) COMMENT '最近1日被支付金额',
    `payment_last_7d_count` BIGINT COMMENT '最近7日被支付次数',
    `payment_last_7d_num` BIGINT COMMENT '最近7日被支付件数',
    `payment_last_7d_amount` DECIMAL(16,2) COMMENT '最近7日被支付金额',
    `payment_last_30d_count` BIGINT COMMENT '最近30日被支付次数',
    `payment_last_30d_num` BIGINT COMMENT '最近30日被支付件数',
    `payment_last_30d_amount` DECIMAL(16,2) COMMENT '最近30日被支付金额',
    `payment_count` BIGINT COMMENT '累积被支付次数',
    `payment_num` BIGINT COMMENT '累积被支付件数',
    `payment_amount` DECIMAL(16,2) COMMENT '累积被支付金额',
    `refund_order_last_1d_count` BIGINT COMMENT '最近1日退单次数',
    `refund_order_last_1d_num` BIGINT COMMENT '最近1日退单件数',
    `refund_order_last_1d_amount` DECIMAL(16,2) COMMENT '最近1日退单金额',
    `refund_order_last_7d_count` BIGINT COMMENT '最近7日退单次数',
    `refund_order_last_7d_num` BIGINT COMMENT '最近7日退单件数',
    `refund_order_last_7d_amount` DECIMAL(16,2) COMMENT '最近7日退单金额',
    `refund_order_last_30d_count` BIGINT COMMENT '最近30日退单次数',
    `refund_order_last_30d_num` BIGINT COMMENT '最近30日退单件数',
    `refund_order_last_30d_amount` DECIMAL(16,2) COMMENT '最近30日退单金额',
    `refund_order_count` BIGINT COMMENT '累积退单次数',
    `refund_order_num` BIGINT COMMENT '累积退单件数',
    `refund_order_amount` DECIMAL(16,2) COMMENT '累积退单金额',
    `refund_payment_last_1d_count` BIGINT COMMENT '最近1日退款次数',
    `refund_payment_last_1d_num` BIGINT COMMENT '最近1日退款件数',
    `refund_payment_last_1d_amount` DECIMAL(16,2) COMMENT '最近1日退款金额',
    `refund_payment_last_7d_count` BIGINT COMMENT '最近7日退款次数',
    `refund_payment_last_7d_num` BIGINT COMMENT '最近7日退款件数',
    `refund_payment_last_7d_amount` DECIMAL(16,2) COMMENT '最近7日退款金额',
    `refund_payment_last_30d_count` BIGINT COMMENT '最近30日退款次数',
    `refund_payment_last_30d_num` BIGINT COMMENT '最近30日退款件数',
    `refund_payment_last_30d_amount` DECIMAL(16,2) COMMENT '最近30日退款金额',
    `refund_payment_count` BIGINT COMMENT '累积退款次数',
    `refund_payment_num` BIGINT COMMENT '累积退款件数',
    `refund_payment_amount` DECIMAL(16,2) COMMENT '累积退款金额',
    `cart_last_1d_count` BIGINT COMMENT '最近1日被加入购物车次数',
    `cart_last_7d_count` BIGINT COMMENT '最近7日被加入购物车次数',
    `cart_last_30d_count` BIGINT COMMENT '最近30日被加入购物车次数',
    `cart_count` BIGINT COMMENT '累积被加入购物车次数',
    `favor_last_1d_count` BIGINT COMMENT '最近1日被收藏次数',
    `favor_last_7d_count` BIGINT COMMENT '最近7日被收藏次数',
    `favor_last_30d_count` BIGINT COMMENT '最近30日被收藏次数',
    `favor_count` BIGINT COMMENT '累积被收藏次数',
    `appraise_last_1d_good_count` BIGINT COMMENT '最近1日好评数',
    `appraise_last_1d_mid_count` BIGINT COMMENT '最近1日中评数',
    `appraise_last_1d_bad_count` BIGINT COMMENT '最近1日差评数',
    `appraise_last_1d_default_count` BIGINT COMMENT '最近1日默认评价数',
    `appraise_last_7d_good_count` BIGINT COMMENT '最近7日好评数',
    `appraise_last_7d_mid_count` BIGINT COMMENT '最近7日中评数',
    `appraise_last_7d_bad_count` BIGINT COMMENT '最近7日差评数',
    `appraise_last_7d_default_count` BIGINT COMMENT '最近7日默认评价数',
    `appraise_last_30d_good_count` BIGINT COMMENT '最近30日好评数',
    `appraise_last_30d_mid_count` BIGINT COMMENT '最近30日中评数',
    `appraise_last_30d_bad_count` BIGINT COMMENT '最近30日差评数',
    `appraise_last_30d_default_count` BIGINT COMMENT '最近30日默认评价数',
    `appraise_good_count` BIGINT COMMENT '累积好评数',
    `appraise_mid_count` BIGINT COMMENT '累积中评数',
    `appraise_bad_count` BIGINT COMMENT '累积差评数',
    `appraise_default_count` BIGINT COMMENT '累积默认评价数'
 )COMMENT '商品主题宽表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall/dwt/dwt_sku_topic/'
TBLPROPERTIES ("parquet.compression"="lzo");

insert overwrite table dwt_sku_topic partition(dt='2020-06-14')
select
    id,
    nvl(order_last_1d_count,0),
    nvl(order_last_1d_num,0),
    nvl(order_activity_last_1d_count,0),
    nvl(order_coupon_last_1d_count,0),
    nvl(order_activity_reduce_last_1d_amount,0),
    nvl(order_coupon_reduce_last_1d_amount,0),
    nvl(order_last_1d_original_amount,0),
    nvl(order_last_1d_final_amount,0),
    nvl(order_last_7d_count,0),
    nvl(order_last_7d_num,0),
    nvl(order_activity_last_7d_count,0),
    nvl(order_coupon_last_7d_count,0),
    nvl(order_activity_reduce_last_7d_amount,0),
    nvl(order_coupon_reduce_last_7d_amount,0),
    nvl(order_last_7d_original_amount,0),
    nvl(order_last_7d_final_amount,0),
    nvl(order_last_30d_count,0),
    nvl(order_last_30d_num,0),
    nvl(order_activity_last_30d_count,0),
    nvl(order_coupon_last_30d_count,0),
    nvl(order_activity_reduce_last_30d_amount,0),
    nvl(order_coupon_reduce_last_30d_amount,0),
    nvl(order_last_30d_original_amount,0),
    nvl(order_last_30d_final_amount,0),
    nvl(order_count,0),
    nvl(order_num,0),
    nvl(order_activity_count,0),
    nvl(order_coupon_count,0),
    nvl(order_activity_reduce_amount,0),
    nvl(order_coupon_reduce_amount,0),
    nvl(order_original_amount,0),
    nvl(order_final_amount,0),
    nvl(payment_last_1d_count,0),
    nvl(payment_last_1d_num,0),
    nvl(payment_last_1d_amount,0),
    nvl(payment_last_7d_count,0),
    nvl(payment_last_7d_num,0),
    nvl(payment_last_7d_amount,0),
    nvl(payment_last_30d_count,0),
    nvl(payment_last_30d_num,0),
    nvl(payment_last_30d_amount,0),
    nvl(payment_count,0),
    nvl(payment_num,0),
    nvl(payment_amount,0),
    nvl(refund_order_last_1d_count,0),
    nvl(refund_order_last_1d_num,0),
    nvl(refund_order_last_1d_amount,0),
    nvl(refund_order_last_7d_count,0),
    nvl(refund_order_last_7d_num,0),
    nvl(refund_order_last_7d_amount,0),
    nvl(refund_order_last_30d_count,0),
    nvl(refund_order_last_30d_num,0),
    nvl(refund_order_last_30d_amount,0),
    nvl(refund_order_count,0),
    nvl(refund_order_num,0),
    nvl(refund_order_amount,0),
    nvl(refund_payment_last_1d_count,0),
    nvl(refund_payment_last_1d_num,0),
    nvl(refund_payment_last_1d_amount,0),
    nvl(refund_payment_last_7d_count,0),
    nvl(refund_payment_last_7d_num,0),
    nvl(refund_payment_last_7d_amount,0),
    nvl(refund_payment_last_30d_count,0),
    nvl(refund_payment_last_30d_num,0),
    nvl(refund_payment_last_30d_amount,0),
    nvl(refund_payment_count,0),
    nvl(refund_payment_num,0),
    nvl(refund_payment_amount,0),
    nvl(cart_last_1d_count,0),
    nvl(cart_last_7d_count,0),
    nvl(cart_last_30d_count,0),
    nvl(cart_count,0),
    nvl(favor_last_1d_count,0),
    nvl(favor_last_7d_count,0),
    nvl(favor_last_30d_count,0),
    nvl(favor_count,0),
    nvl(appraise_last_1d_good_count,0),
    nvl(appraise_last_1d_mid_count,0),
    nvl(appraise_last_1d_bad_count,0),
    nvl(appraise_last_1d_default_count,0),
    nvl(appraise_last_7d_good_count,0),
    nvl(appraise_last_7d_mid_count,0),
    nvl(appraise_last_7d_bad_count,0),
    nvl(appraise_last_7d_default_count,0),
    nvl(appraise_last_30d_good_count,0),
    nvl(appraise_last_30d_mid_count,0),
    nvl(appraise_last_30d_bad_count,0),
    nvl(appraise_last_30d_default_count,0),
    nvl(appraise_good_count,0),
    nvl(appraise_mid_count,0),
    nvl(appraise_bad_count,0),
    nvl(appraise_default_count,0)
from
(
    select
        id
    from dim_sku_info
    where dt='2020-06-14'
)t1
left join
(
    select
        sku_id,
        sum(if(dt='2020-06-14',order_count,0)) order_last_1d_count,
        sum(if(dt='2020-06-14',order_num,0)) order_last_1d_num,
        sum(if(dt='2020-06-14',order_activity_count,0)) order_activity_last_1d_count,
        sum(if(dt='2020-06-14',order_coupon_count,0)) order_coupon_last_1d_count,
        sum(if(dt='2020-06-14',order_activity_reduce_amount,0)) order_activity_reduce_last_1d_amount,
        sum(if(dt='2020-06-14',order_coupon_reduce_amount,0)) order_coupon_reduce_last_1d_amount,
        sum(if(dt='2020-06-14',order_original_amount,0)) order_last_1d_original_amount,
        sum(if(dt='2020-06-14',order_final_amount,0)) order_last_1d_final_amount,
        sum(if(dt>=date_add('2020-06-14',-6),order_count,0)) order_last_7d_count,
        sum(if(dt>=date_add('2020-06-14',-6),order_num,0)) order_last_7d_num,
        sum(if(dt>=date_add('2020-06-14',-6),order_activity_count,0)) order_activity_last_7d_count,
        sum(if(dt>=date_add('2020-06-14',-6),order_coupon_count,0)) order_coupon_last_7d_count,
        sum(if(dt>=date_add('2020-06-14',-6),order_activity_reduce_amount,0)) order_activity_reduce_last_7d_amount,
        sum(if(dt>=date_add('2020-06-14',-6),order_coupon_reduce_amount,0)) order_coupon_reduce_last_7d_amount,
        sum(if(dt>=date_add('2020-06-14',-6),order_original_amount,0)) order_last_7d_original_amount,
        sum(if(dt>=date_add('2020-06-14',-6),order_final_amount,0)) order_last_7d_final_amount,
        sum(if(dt>=date_add('2020-06-14',-29),order_count,0)) order_last_30d_count,
        sum(if(dt>=date_add('2020-06-14',-29),order_num,0)) order_last_30d_num,
        sum(if(dt>=date_add('2020-06-14',-29),order_activity_count,0)) order_activity_last_30d_count,
        sum(if(dt>=date_add('2020-06-14',-29),order_coupon_count,0)) order_coupon_last_30d_count,
        sum(if(dt>=date_add('2020-06-14',-29),order_activity_reduce_amount,0)) order_activity_reduce_last_30d_amount,
        sum(if(dt>=date_add('2020-06-14',-29),order_coupon_reduce_amount,0)) order_coupon_reduce_last_30d_amount,
        sum(if(dt>=date_add('2020-06-14',-29),order_original_amount,0)) order_last_30d_original_amount,
        sum(if(dt>=date_add('2020-06-14',-29),order_final_amount,0)) order_last_30d_final_amount,
        sum(order_count) order_count,
        sum(order_num) order_num,
        sum(order_activity_count) order_activity_count,
        sum(order_coupon_count) order_coupon_count,
        sum(order_activity_reduce_amount) order_activity_reduce_amount,
        sum(order_coupon_reduce_amount) order_coupon_reduce_amount,
        sum(order_original_amount) order_original_amount,
        sum(order_final_amount) order_final_amount,
        sum(if(dt='2020-06-14',payment_count,0)) payment_last_1d_count,
        sum(if(dt='2020-06-14',payment_num,0)) payment_last_1d_num,
        sum(if(dt='2020-06-14',payment_amount,0)) payment_last_1d_amount,
        sum(if(dt>=date_add('2020-06-14',-6),payment_count,0)) payment_last_7d_count,
        sum(if(dt>=date_add('2020-06-14',-6),payment_num,0)) payment_last_7d_num,
        sum(if(dt>=date_add('2020-06-14',-6),payment_amount,0)) payment_last_7d_amount,
        sum(if(dt>=date_add('2020-06-14',-29),payment_count,0)) payment_last_30d_count,
        sum(if(dt>=date_add('2020-06-14',-29),payment_num,0)) payment_last_30d_num,
        sum(if(dt>=date_add('2020-06-14',-29),payment_amount,0)) payment_last_30d_amount,
        sum(payment_count) payment_count,
        sum(payment_num) payment_num,
        sum(payment_amount) payment_amount,
        sum(if(dt='2020-06-14',refund_order_count,0)) refund_order_last_1d_count,
        sum(if(dt='2020-06-14',refund_order_num,0)) refund_order_last_1d_num,
        sum(if(dt='2020-06-14',refund_order_amount,0)) refund_order_last_1d_amount,
        sum(if(dt>=date_add('2020-06-14',-6),refund_order_count,0)) refund_order_last_7d_count,
        sum(if(dt>=date_add('2020-06-14',-6),refund_order_num,0)) refund_order_last_7d_num,
        sum(if(dt>=date_add('2020-06-14',-6),refund_order_amount,0)) refund_order_last_7d_amount,
        sum(if(dt>=date_add('2020-06-14',-29),refund_order_count,0)) refund_order_last_30d_count,
        sum(if(dt>=date_add('2020-06-14',-29),refund_order_num,0)) refund_order_last_30d_num,
        sum(if(dt>=date_add('2020-06-14',-29),refund_order_amount,0)) refund_order_last_30d_amount,
        sum(refund_order_count) refund_order_count,
        sum(refund_order_num) refund_order_num,
        sum(refund_order_amount) refund_order_amount,
        sum(if(dt='2020-06-14',refund_payment_count,0)) refund_payment_last_1d_count,
        sum(if(dt='2020-06-14',refund_payment_num,0)) refund_payment_last_1d_num,
        sum(if(dt='2020-06-14',refund_payment_amount,0)) refund_payment_last_1d_amount,
        sum(if(dt>=date_add('2020-06-14',-6),refund_payment_count,0)) refund_payment_last_7d_count,
        sum(if(dt>=date_add('2020-06-14',-6),refund_payment_num,0)) refund_payment_last_7d_num,
        sum(if(dt>=date_add('2020-06-14',-6),refund_payment_amount,0)) refund_payment_last_7d_amount,
        sum(if(dt>=date_add('2020-06-14',-29),refund_payment_count,0)) refund_payment_last_30d_count,
        sum(if(dt>=date_add('2020-06-14',-29),refund_payment_num,0)) refund_payment_last_30d_num,
        sum(if(dt>=date_add('2020-06-14',-29),refund_payment_amount,0)) refund_payment_last_30d_amount,
        sum(refund_payment_count) refund_payment_count,
        sum(refund_payment_num) refund_payment_num,
        sum(refund_payment_amount) refund_payment_amount,
        sum(if(dt='2020-06-14',cart_count,0)) cart_last_1d_count,
        sum(if(dt>=date_add('2020-06-14',-6),cart_count,0)) cart_last_7d_count,
        sum(if(dt>=date_add('2020-06-14',-29),cart_count,0)) cart_last_30d_count,
        sum(cart_count) cart_count,
        sum(if(dt='2020-06-14',favor_count,0)) favor_last_1d_count,
        sum(if(dt>=date_add('2020-06-14',-6),favor_count,0)) favor_last_7d_count,
        sum(if(dt>=date_add('2020-06-14',-29),favor_count,0)) favor_last_30d_count,
        sum(favor_count) favor_count,
        sum(if(dt='2020-06-14',appraise_good_count,0)) appraise_last_1d_good_count,
        sum(if(dt='2020-06-14',appraise_mid_count,0)) appraise_last_1d_mid_count,
        sum(if(dt='2020-06-14',appraise_bad_count,0)) appraise_last_1d_bad_count,
        sum(if(dt='2020-06-14',appraise_default_count,0)) appraise_last_1d_default_count,
        sum(if(dt>=date_add('2020-06-14',-6),appraise_good_count,0)) appraise_last_7d_good_count,
        sum(if(dt>=date_add('2020-06-14',-6),appraise_mid_count,0)) appraise_last_7d_mid_count,
        sum(if(dt>=date_add('2020-06-14',-6),appraise_bad_count,0)) appraise_last_7d_bad_count,
        sum(if(dt>=date_add('2020-06-14',-6),appraise_default_count,0)) appraise_last_7d_default_count,
        sum(if(dt>=date_add('2020-06-14',-29),appraise_good_count,0)) appraise_last_30d_good_count,
        sum(if(dt>=date_add('2020-06-14',-29),appraise_mid_count,0)) appraise_last_30d_mid_count,
        sum(if(dt>=date_add('2020-06-14',-29),appraise_bad_count,0)) appraise_last_30d_bad_count,
        sum(if(dt>=date_add('2020-06-14',-29),appraise_default_count,0)) appraise_last_30d_default_count,
        sum(appraise_good_count) appraise_good_count,
        sum(appraise_mid_count) appraise_mid_count,
        sum(appraise_bad_count) appraise_bad_count,
        sum(appraise_default_count) appraise_default_count
    from dws_sku_action_daycount
    group by sku_id
)t2
on t1.id=t2.sku_id;

insert overwrite table dwt_sku_topic partition(dt='2020-06-15')
select
    nvl(1d_ago.sku_id,old.sku_id),
    nvl(1d_ago.order_count,0),
    nvl(1d_ago.order_num,0),
    nvl(1d_ago.order_activity_count,0),
    nvl(1d_ago.order_coupon_count,0),
    nvl(1d_ago.order_activity_reduce_amount,0.0),
    nvl(1d_ago.order_coupon_reduce_amount,0.0),
    nvl(1d_ago.order_original_amount,0.0),
    nvl(1d_ago.order_final_amount,0.0),
    nvl(old.order_last_7d_count,0)+nvl(1d_ago.order_count,0)- nvl(7d_ago.order_count,0),
    nvl(old.order_last_7d_num,0)+nvl(1d_ago.order_num,0)- nvl(7d_ago.order_num,0),
    nvl(old.order_activity_last_7d_count,0)+nvl(1d_ago.order_activity_count,0)- nvl(7d_ago.order_activity_count,0),
    nvl(old.order_coupon_last_7d_count,0)+nvl(1d_ago.order_coupon_count,0)- nvl(7d_ago.order_coupon_count,0),
    nvl(old.order_activity_reduce_last_7d_amount,0.0)+nvl(1d_ago.order_activity_reduce_amount,0.0)- nvl(7d_ago.order_activity_reduce_amount,0.0),
    nvl(old.order_coupon_reduce_last_7d_amount,0.0)+nvl(1d_ago.order_coupon_reduce_amount,0.0)- nvl(7d_ago.order_coupon_reduce_amount,0.0),
    nvl(old.order_last_7d_original_amount,0.0)+nvl(1d_ago.order_original_amount,0.0)- nvl(7d_ago.order_original_amount,0.0),
    nvl(old.order_last_7d_final_amount,0.0)+nvl(1d_ago.order_final_amount,0.0)- nvl(7d_ago.order_final_amount,0.0),
    nvl(old.order_last_30d_count,0)+nvl(1d_ago.order_count,0)- nvl(30d_ago.order_count,0),
    nvl(old.order_last_30d_num,0)+nvl(1d_ago.order_num,0)- nvl(30d_ago.order_num,0),
    nvl(old.order_activity_last_30d_count,0)+nvl(1d_ago.order_activity_count,0)- nvl(30d_ago.order_activity_count,0),
    nvl(old.order_coupon_last_30d_count,0)+nvl(1d_ago.order_coupon_count,0)- nvl(30d_ago.order_coupon_count,0),
    nvl(old.order_activity_reduce_last_30d_amount,0.0)+nvl(1d_ago.order_activity_reduce_amount,0.0)- nvl(30d_ago.order_activity_reduce_amount,0.0),
    nvl(old.order_coupon_reduce_last_30d_amount,0.0)+nvl(1d_ago.order_coupon_reduce_amount,0.0)- nvl(30d_ago.order_coupon_reduce_amount,0.0),
    nvl(old.order_last_30d_original_amount,0.0)+nvl(1d_ago.order_original_amount,0.0)- nvl(30d_ago.order_original_amount,0.0),
    nvl(old.order_last_30d_final_amount,0.0)+nvl(1d_ago.order_final_amount,0.0)- nvl(30d_ago.order_final_amount,0.0),
    nvl(old.order_count,0)+nvl(1d_ago.order_count,0),
    nvl(old.order_num,0)+nvl(1d_ago.order_num,0),
    nvl(old.order_activity_count,0)+nvl(1d_ago.order_activity_count,0),
    nvl(old.order_coupon_count,0)+nvl(1d_ago.order_coupon_count,0),
    nvl(old.order_activity_reduce_amount,0.0)+nvl(1d_ago.order_activity_reduce_amount,0.0),
    nvl(old.order_coupon_reduce_amount,0.0)+nvl(1d_ago.order_coupon_reduce_amount,0.0),
    nvl(old.order_original_amount,0.0)+nvl(1d_ago.order_original_amount,0.0),
    nvl(old.order_final_amount,0.0)+nvl(1d_ago.order_final_amount,0.0),
    nvl(1d_ago.payment_count,0),
    nvl(1d_ago.payment_num,0),
    nvl(1d_ago.payment_amount,0.0),
    nvl(old.payment_last_7d_count,0)+nvl(1d_ago.payment_count,0)- nvl(7d_ago.payment_count,0),
    nvl(old.payment_last_7d_num,0)+nvl(1d_ago.payment_num,0)- nvl(7d_ago.payment_num,0),
    nvl(old.payment_last_7d_amount,0.0)+nvl(1d_ago.payment_amount,0.0)- nvl(7d_ago.payment_amount,0.0),
    nvl(old.payment_last_30d_count,0)+nvl(1d_ago.payment_count,0)- nvl(30d_ago.payment_count,0),
    nvl(old.payment_last_30d_num,0)+nvl(1d_ago.payment_num,0)- nvl(30d_ago.payment_num,0),
    nvl(old.payment_last_30d_amount,0.0)+nvl(1d_ago.payment_amount,0.0)- nvl(30d_ago.payment_amount,0.0),
    nvl(old.payment_count,0)+nvl(1d_ago.payment_count,0),
    nvl(old.payment_num,0)+nvl(1d_ago.payment_num,0),
    nvl(old.payment_amount,0.0)+nvl(1d_ago.payment_amount,0.0),
    nvl(1d_ago.refund_order_count,0),
    nvl(1d_ago.refund_order_num,0),
    nvl(1d_ago.refund_order_amount,0.0),
    nvl(old.refund_order_last_7d_count,0)+nvl(1d_ago.refund_order_count,0)- nvl(7d_ago.refund_order_count,0),
    nvl(old.refund_order_last_7d_num,0)+nvl(1d_ago.refund_order_num,0)- nvl(7d_ago.refund_order_num,0),
    nvl(old.refund_order_last_7d_amount,0.0)+nvl(1d_ago.refund_order_amount,0.0)- nvl(7d_ago.refund_order_amount,0.0),
    nvl(old.refund_order_last_30d_count,0)+nvl(1d_ago.refund_order_count,0)- nvl(30d_ago.refund_order_count,0),
    nvl(old.refund_order_last_30d_num,0)+nvl(1d_ago.refund_order_num,0)- nvl(30d_ago.refund_order_num,0),
    nvl(old.refund_order_last_30d_amount,0.0)+nvl(1d_ago.refund_order_amount,0.0)- nvl(30d_ago.refund_order_amount,0.0),
    nvl(old.refund_order_count,0)+nvl(1d_ago.refund_order_count,0),
    nvl(old.refund_order_num,0)+nvl(1d_ago.refund_order_num,0),
    nvl(old.refund_order_amount,0.0)+nvl(1d_ago.refund_order_amount,0.0),
    nvl(1d_ago.refund_payment_count,0),
    nvl(1d_ago.refund_payment_num,0),
    nvl(1d_ago.refund_payment_amount,0.0),
    nvl(old.refund_payment_last_7d_count,0)+nvl(1d_ago.refund_payment_count,0)- nvl(7d_ago.refund_payment_count,0),
    nvl(old.refund_payment_last_7d_num,0)+nvl(1d_ago.refund_payment_num,0)- nvl(7d_ago.refund_payment_num,0),
    nvl(old.refund_payment_last_7d_amount,0.0)+nvl(1d_ago.refund_payment_amount,0.0)- nvl(7d_ago.refund_payment_amount,0.0),
    nvl(old.refund_payment_last_30d_count,0)+nvl(1d_ago.refund_payment_count,0)- nvl(30d_ago.refund_payment_count,0),
    nvl(old.refund_payment_last_30d_num,0)+nvl(1d_ago.refund_payment_num,0)- nvl(30d_ago.refund_payment_num,0),
    nvl(old.refund_payment_last_30d_amount,0.0)+nvl(1d_ago.refund_payment_amount,0.0)- nvl(30d_ago.refund_payment_amount,0.0),
    nvl(old.refund_payment_count,0)+nvl(1d_ago.refund_payment_count,0),
    nvl(old.refund_payment_num,0)+nvl(1d_ago.refund_payment_num,0),
    nvl(old.refund_payment_amount,0.0)+nvl(1d_ago.refund_payment_amount,0.0),
    nvl(1d_ago.cart_count,0),
    nvl(old.cart_last_7d_count,0)+nvl(1d_ago.cart_count,0)- nvl(7d_ago.cart_count,0),
    nvl(old.cart_last_30d_count,0)+nvl(1d_ago.cart_count,0)- nvl(30d_ago.cart_count,0),
    nvl(old.cart_count,0)+nvl(1d_ago.cart_count,0),
    nvl(1d_ago.favor_count,0),
    nvl(old.favor_last_7d_count,0)+nvl(1d_ago.favor_count,0)- nvl(7d_ago.favor_count,0),
    nvl(old.favor_last_30d_count,0)+nvl(1d_ago.favor_count,0)- nvl(30d_ago.favor_count,0),
    nvl(old.favor_count,0)+nvl(1d_ago.favor_count,0),
    nvl(1d_ago.appraise_good_count,0),
    nvl(1d_ago.appraise_mid_count,0),
    nvl(1d_ago.appraise_bad_count,0),
    nvl(1d_ago.appraise_default_count,0),
    nvl(old.appraise_last_7d_good_count,0)+nvl(1d_ago.appraise_good_count,0)- nvl(7d_ago.appraise_good_count,0),
    nvl(old.appraise_last_7d_mid_count,0)+nvl(1d_ago.appraise_mid_count,0)- nvl(7d_ago.appraise_mid_count,0),
    nvl(old.appraise_last_7d_bad_count,0)+nvl(1d_ago.appraise_bad_count,0)- nvl(7d_ago.appraise_bad_count,0),
    nvl(old.appraise_last_7d_default_count,0)+nvl(1d_ago.appraise_default_count,0)- nvl(7d_ago.appraise_default_count,0),
    nvl(old.appraise_last_30d_good_count,0)+nvl(1d_ago.appraise_good_count,0)- nvl(30d_ago.appraise_good_count,0),
    nvl(old.appraise_last_30d_mid_count,0)+nvl(1d_ago.appraise_mid_count,0)- nvl(30d_ago.appraise_mid_count,0),
    nvl(old.appraise_last_30d_bad_count,0)+nvl(1d_ago.appraise_bad_count,0)- nvl(30d_ago.appraise_bad_count,0),
    nvl(old.appraise_last_30d_default_count,0)+nvl(1d_ago.appraise_default_count,0)- nvl(30d_ago.appraise_default_count,0),
    nvl(old.appraise_good_count,0)+nvl(1d_ago.appraise_good_count,0),
    nvl(old.appraise_mid_count,0)+nvl(1d_ago.appraise_mid_count,0),
    nvl(old.appraise_bad_count,0)+nvl(1d_ago.appraise_bad_count,0),
    nvl(old.appraise_default_count,0)+nvl(1d_ago.appraise_default_count,0)
from
(
    select
        sku_id,
        order_last_1d_count,
        order_last_1d_num,
        order_activity_last_1d_count,
        order_coupon_last_1d_count,
        order_activity_reduce_last_1d_amount,
        order_coupon_reduce_last_1d_amount,
        order_last_1d_original_amount,
        order_last_1d_final_amount,
        order_last_7d_count,
        order_last_7d_num,
        order_activity_last_7d_count,
        order_coupon_last_7d_count,
        order_activity_reduce_last_7d_amount,
        order_coupon_reduce_last_7d_amount,
        order_last_7d_original_amount,
        order_last_7d_final_amount,
        order_last_30d_count,
        order_last_30d_num,
        order_activity_last_30d_count,
        order_coupon_last_30d_count,
        order_activity_reduce_last_30d_amount,
        order_coupon_reduce_last_30d_amount,
        order_last_30d_original_amount,
        order_last_30d_final_amount,
        order_count,
        order_num,
        order_activity_count,
        order_coupon_count,
        order_activity_reduce_amount,
        order_coupon_reduce_amount,
        order_original_amount,
        order_final_amount,
        payment_last_1d_count,
        payment_last_1d_num,
        payment_last_1d_amount,
        payment_last_7d_count,
        payment_last_7d_num,
        payment_last_7d_amount,
        payment_last_30d_count,
        payment_last_30d_num,
        payment_last_30d_amount,
        payment_count,
        payment_num,
        payment_amount,
        refund_order_last_1d_count,
        refund_order_last_1d_num,
        refund_order_last_1d_amount,
        refund_order_last_7d_count,
        refund_order_last_7d_num,
        refund_order_last_7d_amount,
        refund_order_last_30d_count,
        refund_order_last_30d_num,
        refund_order_last_30d_amount,
        refund_order_count,
        refund_order_num,
        refund_order_amount,
        refund_payment_last_1d_count,
        refund_payment_last_1d_num,
        refund_payment_last_1d_amount,
        refund_payment_last_7d_count,
        refund_payment_last_7d_num,
        refund_payment_last_7d_amount,
        refund_payment_last_30d_count,
        refund_payment_last_30d_num,
        refund_payment_last_30d_amount,
        refund_payment_count,
        refund_payment_num,
        refund_payment_amount,
        cart_last_1d_count,
        cart_last_7d_count,
        cart_last_30d_count,
        cart_count,
        favor_last_1d_count,
        favor_last_7d_count,
        favor_last_30d_count,
        favor_count,
        appraise_last_1d_good_count,
        appraise_last_1d_mid_count,
        appraise_last_1d_bad_count,
        appraise_last_1d_default_count,
        appraise_last_7d_good_count,
        appraise_last_7d_mid_count,
        appraise_last_7d_bad_count,
        appraise_last_7d_default_count,
        appraise_last_30d_good_count,
        appraise_last_30d_mid_count,
        appraise_last_30d_bad_count,
        appraise_last_30d_default_count,
        appraise_good_count,
        appraise_mid_count,
        appraise_bad_count,
        appraise_default_count
    from dwt_sku_topic
    where dt=date_add('2020-06-15',-1)
)old
full outer join
(
    select
        sku_id,
        order_count,
        order_num,
        order_activity_count,
        order_coupon_count,
        order_activity_reduce_amount,
        order_coupon_reduce_amount,
        order_original_amount,
        order_final_amount,
        payment_count,
        payment_num,
        payment_amount,
        refund_order_count,
        refund_order_num,
        refund_order_amount,
        refund_payment_count,
        refund_payment_num,
        refund_payment_amount,
        cart_count,
        favor_count,
        appraise_good_count,
        appraise_mid_count,
        appraise_bad_count,
        appraise_default_count
    from dws_sku_action_daycount
    where dt='2020-06-15'
)1d_ago
on old.sku_id=1d_ago.sku_id
left join
(
    select
        sku_id,
        order_count,
        order_num,
        order_activity_count,
        order_coupon_count,
        order_activity_reduce_amount,
        order_coupon_reduce_amount,
        order_original_amount,
        order_final_amount,
        payment_count,
        payment_num,
        payment_amount,
        refund_order_count,
        refund_order_num,
        refund_order_amount,
        refund_payment_count,
        refund_payment_num,
        refund_payment_amount,
        cart_count,
        favor_count,
        appraise_good_count,
        appraise_mid_count,
        appraise_bad_count,
        appraise_default_count
    from dws_sku_action_daycount
    where dt=date_add('2020-06-15',-7)
)7d_ago
on old.sku_id=7d_ago.sku_id
left join
(
    select
        sku_id,
        order_count,
        order_num,
        order_activity_count,
        order_coupon_count,
        order_activity_reduce_amount,
        order_coupon_reduce_amount,
        order_original_amount,
        order_final_amount,
        payment_count,
        payment_num,
        payment_amount,
        refund_order_count,
        refund_order_num,
        refund_order_amount,
        refund_payment_count,
        refund_payment_num,
        refund_payment_amount,
        cart_count,
        favor_count,
        appraise_good_count,
        appraise_mid_count,
        appraise_bad_count,
        appraise_default_count
    from dws_sku_action_daycount
    where dt=date_add('2020-06-15',-30)
)30d_ago
on old.sku_id=30d_ago.sku_id;

---------------------------------------dwt_coupon_topic----------------------------------
DROP TABLE IF EXISTS dwt_coupon_topic;
CREATE EXTERNAL TABLE dwt_coupon_topic(
    `coupon_id` STRING COMMENT '优惠券ID',
    `get_last_1d_count` BIGINT COMMENT '最近1日领取次数',
    `get_last_7d_count` BIGINT COMMENT '最近7日领取次数',
    `get_last_30d_count` BIGINT COMMENT '最近30日领取次数',
    `get_count` BIGINT COMMENT '累积领取次数',
    `order_last_1d_count` BIGINT COMMENT '最近1日使用某券下单次数',
    `order_last_1d_reduce_amount` DECIMAL(16,2) COMMENT '最近1日使用某券下单优惠金额',
    `order_last_1d_original_amount` DECIMAL(16,2) COMMENT '最近1日使用某券下单原始金额',
    `order_last_1d_final_amount` DECIMAL(16,2) COMMENT '最近1日使用某券下单最终金额',
    `order_last_7d_count` BIGINT COMMENT '最近7日使用某券下单次数',
    `order_last_7d_reduce_amount` DECIMAL(16,2) COMMENT '最近7日使用某券下单优惠金额',
    `order_last_7d_original_amount` DECIMAL(16,2) COMMENT '最近7日使用某券下单原始金额',
    `order_last_7d_final_amount` DECIMAL(16,2) COMMENT '最近7日使用某券下单最终金额',
    `order_last_30d_count` BIGINT COMMENT '最近30日使用某券下单次数',
    `order_last_30d_reduce_amount` DECIMAL(16,2) COMMENT '最近30日使用某券下单优惠金额',
    `order_last_30d_original_amount` DECIMAL(16,2) COMMENT '最近30日使用某券下单原始金额',
    `order_last_30d_final_amount` DECIMAL(16,2) COMMENT '最近30日使用某券下单最终金额',
    `order_count` BIGINT COMMENT '累积使用(下单)次数',
    `order_reduce_amount` DECIMAL(16,2) COMMENT '使用某券累积下单优惠金额',
    `order_original_amount` DECIMAL(16,2) COMMENT '使用某券累积下单原始金额',
    `order_final_amount` DECIMAL(16,2) COMMENT '使用某券累积下单最终金额',
    `payment_last_1d_count` BIGINT COMMENT '最近1日使用某券支付次数',
    `payment_last_1d_reduce_amount` DECIMAL(16,2) COMMENT '最近1日使用某券优惠金额',
    `payment_last_1d_amount` DECIMAL(16,2) COMMENT '最近1日使用某券支付金额',
    `payment_last_7d_count` BIGINT COMMENT '最近7日使用某券支付次数',
    `payment_last_7d_reduce_amount` DECIMAL(16,2) COMMENT '最近7日使用某券优惠金额',
    `payment_last_7d_amount` DECIMAL(16,2) COMMENT '最近7日使用某券支付金额',
    `payment_last_30d_count` BIGINT COMMENT '最近30日使用某券支付次数',
    `payment_last_30d_reduce_amount` DECIMAL(16,2) COMMENT '最近30日使用某券优惠金额',
    `payment_last_30d_amount` DECIMAL(16,2) COMMENT '最近30日使用某券支付金额',
    `payment_count` BIGINT COMMENT '累积使用(支付)次数',
    `payment_reduce_amount` DECIMAL(16,2) COMMENT '使用某券累积优惠金额',
    `payment_amount` DECIMAL(16,2) COMMENT '使用某券累积支付金额',
    `expire_last_1d_count` BIGINT COMMENT '最近1日过期次数',
    `expire_last_7d_count` BIGINT COMMENT '最近7日过期次数',
    `expire_last_30d_count` BIGINT COMMENT '最近30日过期次数',
    `expire_count` BIGINT COMMENT '累积过期次数'
)comment '优惠券主题表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall/dwt/dwt_coupon_topic/'
TBLPROPERTIES ("parquet.compression"="lzo");

insert overwrite table dwt_coupon_topic partition(dt='2020-06-14')
select
    id,
    nvl(get_last_1d_count,0),
    nvl(get_last_7d_count,0),
    nvl(get_last_30d_count,0),
    nvl(get_count,0),
    nvl(order_last_1d_count,0),
    nvl(order_last_1d_reduce_amount,0),
    nvl(order_last_1d_original_amount,0),
    nvl(order_last_1d_final_amount,0),
    nvl(order_last_7d_count,0),
    nvl(order_last_7d_reduce_amount,0),
    nvl(order_last_7d_original_amount,0),
    nvl(order_last_7d_final_amount,0),
    nvl(order_last_30d_count,0),
    nvl(order_last_30d_reduce_amount,0),
    nvl(order_last_30d_original_amount,0),
    nvl(order_last_30d_final_amount,0),
    nvl(order_count,0),
    nvl(order_reduce_amount,0),
    nvl(order_original_amount,0),
    nvl(order_final_amount,0),
    nvl(payment_last_1d_count,0),
    nvl(payment_last_1d_reduce_amount,0),
    nvl(payment_last_1d_amount,0),
    nvl(payment_last_7d_count,0),
    nvl(payment_last_7d_reduce_amount,0),
    nvl(payment_last_7d_amount,0),
    nvl(payment_last_30d_count,0),
    nvl(payment_last_30d_reduce_amount,0),
    nvl(payment_last_30d_amount,0),
    nvl(payment_count,0),
    nvl(payment_reduce_amount,0),
    nvl(payment_amount,0),
    nvl(expire_last_1d_count,0),
    nvl(expire_last_7d_count,0),
    nvl(expire_last_30d_count,0),
    nvl(expire_count,0)
from
(
    select
        id
    from dim_coupon_info
    where dt='2020-06-14'
)t1
left join
(
    select
        coupon_id coupon_id,
        sum(if(dt='2020-06-14',get_count,0)) get_last_1d_count,
        sum(if(dt>=date_add('2020-06-14',-6),get_count,0)) get_last_7d_count,
        sum(if(dt>=date_add('2020-06-14',-29),get_count,0)) get_last_30d_count,
        sum(get_count) get_count,
        sum(if(dt='2020-06-14',order_count,0)) order_last_1d_count,
        sum(if(dt='2020-06-14',order_reduce_amount,0)) order_last_1d_reduce_amount,
        sum(if(dt='2020-06-14',order_original_amount,0)) order_last_1d_original_amount,
        sum(if(dt='2020-06-14',order_final_amount,0)) order_last_1d_final_amount,
        sum(if(dt>=date_add('2020-06-14',-6),order_count,0)) order_last_7d_count,
        sum(if(dt>=date_add('2020-06-14',-6),order_reduce_amount,0)) order_last_7d_reduce_amount,
        sum(if(dt>=date_add('2020-06-14',-6),order_original_amount,0)) order_last_7d_original_amount,
        sum(if(dt>=date_add('2020-06-14',-6),order_final_amount,0)) order_last_7d_final_amount,
        sum(if(dt>=date_add('2020-06-14',-29),order_count,0)) order_last_30d_count,
        sum(if(dt>=date_add('2020-06-14',-29),order_reduce_amount,0)) order_last_30d_reduce_amount,
        sum(if(dt>=date_add('2020-06-14',-29),order_original_amount,0)) order_last_30d_original_amount,
        sum(if(dt>=date_add('2020-06-14',-29),order_final_amount,0)) order_last_30d_final_amount,
        sum(order_count) order_count,
        sum(order_reduce_amount) order_reduce_amount,
        sum(order_original_amount) order_original_amount,
        sum(order_final_amount) order_final_amount,
        sum(if(dt='2020-06-14',payment_count,0)) payment_last_1d_count,
        sum(if(dt='2020-06-14',payment_reduce_amount,0)) payment_last_1d_reduce_amount,
        sum(if(dt='2020-06-14',payment_amount,0)) payment_last_1d_amount,
        sum(if(dt>=date_add('2020-06-14',-6),payment_count,0)) payment_last_7d_count,
        sum(if(dt>=date_add('2020-06-14',-6),payment_reduce_amount,0)) payment_last_7d_reduce_amount,
        sum(if(dt>=date_add('2020-06-14',-6),payment_amount,0)) payment_last_7d_amount,
        sum(if(dt>=date_add('2020-06-14',-29),payment_count,0)) payment_last_30d_count,
        sum(if(dt>=date_add('2020-06-14',-29),payment_reduce_amount,0)) payment_last_30d_reduce_amount,
        sum(if(dt>=date_add('2020-06-14',-29),payment_amount,0)) payment_last_30d_amount,
        sum(payment_count) payment_count,
        sum(payment_reduce_amount) payment_reduce_amount,
        sum(payment_amount) payment_amount,
        sum(if(dt='2020-06-14',expire_count,0)) expire_last_1d_count,
        sum(if(dt>=date_add('2020-06-14',-6),expire_count,0)) expire_last_7d_count,
        sum(if(dt>=date_add('2020-06-14',-29),expire_count,0)) expire_last_30d_count,
        sum(expire_count) expire_count
    from dws_coupon_info_daycount
    group by coupon_id
)t2
on t1.id=t2.coupon_id;

---------------------------------------dwt_activity_topic----------------------------------
DROP TABLE IF EXISTS dwt_activity_topic;
CREATE EXTERNAL TABLE dwt_activity_topic(
    `activity_rule_id` STRING COMMENT '活动规则ID',
    `activity_id` STRING  COMMENT '活动ID',
    `order_last_1d_count` BIGINT COMMENT '最近1日参与某活动某规则下单次数',
    `order_last_1d_reduce_amount` DECIMAL(16,2) COMMENT '最近1日参与某活动某规则下单优惠金额',
    `order_last_1d_original_amount` DECIMAL(16,2) COMMENT '最近1日参与某活动某规则下单原始金额',
    `order_last_1d_final_amount` DECIMAL(16,2) COMMENT '最近1日参与某活动某规则下单最终金额',
    `order_count` BIGINT COMMENT '参与某活动某规则累积下单次数',
    `order_reduce_amount` DECIMAL(16,2) COMMENT '参与某活动某规则累积下单优惠金额',
    `order_original_amount` DECIMAL(16,2) COMMENT '参与某活动某规则累积下单原始金额',
    `order_final_amount` DECIMAL(16,2) COMMENT '参与某活动某规则累积下单最终金额',
    `payment_last_1d_count` BIGINT COMMENT '最近1日参与某活动某规则支付次数',
    `payment_last_1d_reduce_amount` DECIMAL(16,2) COMMENT '最近1日参与某活动某规则支付优惠金额',
    `payment_last_1d_amount` DECIMAL(16,2) COMMENT '最近1日参与某活动某规则支付金额',
    `payment_count` BIGINT COMMENT '参与某活动某规则累积支付次数',
    `payment_reduce_amount` DECIMAL(16,2) COMMENT '参与某活动某规则累积支付优惠金额',
    `payment_amount` DECIMAL(16,2) COMMENT '参与某活动某规则累积支付金额'
) COMMENT '活动主题宽表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall/dwt/dwt_activity_topic/'
TBLPROPERTIES ("parquet.compression"="lzo");

insert overwrite table dwt_activity_topic partition(dt='2020-06-14')
select
    t1.activity_rule_id,
    t1.activity_id,
    nvl(order_last_1d_count,0),
    nvl(order_last_1d_reduce_amount,0),
    nvl(order_last_1d_original_amount,0),
    nvl(order_last_1d_final_amount,0),
    nvl(order_count,0),
    nvl(order_reduce_amount,0),
    nvl(order_original_amount,0),
    nvl(order_final_amount,0),
    nvl(payment_last_1d_count,0),
    nvl(payment_last_1d_reduce_amount,0),
    nvl(payment_last_1d_amount,0),
    nvl(payment_count,0),
    nvl(payment_reduce_amount,0),
    nvl(payment_amount,0)
from
(
    select
        activity_rule_id,
        activity_id
    from dim_activity_rule_info
    where dt='2020-06-14'
)t1
left join
(
    select
        activity_rule_id,
        activity_id,
        sum(if(dt='2020-06-14',order_count,0)) order_last_1d_count,
        sum(if(dt='2020-06-14',order_reduce_amount,0)) order_last_1d_reduce_amount,
        sum(if(dt='2020-06-14',order_original_amount,0)) order_last_1d_original_amount,
        sum(if(dt='2020-06-14',order_final_amount,0)) order_last_1d_final_amount,
        sum(order_count) order_count,
        sum(order_reduce_amount) order_reduce_amount,
        sum(order_original_amount) order_original_amount,
        sum(order_final_amount) order_final_amount,
        sum(if(dt='2020-06-14',payment_count,0)) payment_last_1d_count,
        sum(if(dt='2020-06-14',payment_reduce_amount,0)) payment_last_1d_reduce_amount,
        sum(if(dt='2020-06-14',payment_amount,0)) payment_last_1d_amount,
        sum(payment_count) payment_count,
        sum(payment_reduce_amount) payment_reduce_amount,
        sum(payment_amount) payment_amount
    from dws_activity_info_daycount
    group by activity_rule_id,activity_id
)t2
on t1.activity_rule_id=t2.activity_rule_id
and t1.activity_id=t2.activity_id;
（2）每日装载
insert overwrite table dwt_activity_topic partition(dt='2020-06-15')
select
    nvl(1d_ago.activity_rule_id,old.activity_rule_id),
    nvl(1d_ago.activity_id,old.activity_id),
    nvl(1d_ago.order_count,0),
    nvl(1d_ago.order_reduce_amount,0.0),
    nvl(1d_ago.order_original_amount,0.0),
    nvl(1d_ago.order_final_amount,0.0),
    nvl(old.order_count,0)+nvl(1d_ago.order_count,0),
    nvl(old.order_reduce_amount,0.0)+nvl(1d_ago.order_reduce_amount,0.0),
    nvl(old.order_original_amount,0.0)+nvl(1d_ago.order_original_amount,0.0),
    nvl(old.order_final_amount,0.0)+nvl(1d_ago.order_final_amount,0.0),
    nvl(1d_ago.payment_count,0),
    nvl(1d_ago.payment_reduce_amount,0.0),
    nvl(1d_ago.payment_amount,0.0),
    nvl(old.payment_count,0)+nvl(1d_ago.payment_count,0),
    nvl(old.payment_reduce_amount,0.0)+nvl(1d_ago.payment_reduce_amount,0.0),
    nvl(old.payment_amount,0.0)+nvl(1d_ago.payment_amount,0.0)
from
(
    select
        activity_rule_id,
        activity_id,
        order_count,
        order_reduce_amount,
        order_original_amount,
        order_final_amount,
        payment_count,
        payment_reduce_amount,
        payment_amount
    from dwt_activity_topic
    where dt=date_add('2020-06-15',-1)
)old
full outer join
(
    select
        activity_rule_id,
        activity_id,
        order_count,
        order_reduce_amount,
        order_original_amount,
        order_final_amount,
        payment_count,
        payment_reduce_amount,
        payment_amount
    from dws_activity_info_daycount
    where dt='2020-06-15'
)1d_ago
on old.activity_rule_id=1d_ago.activity_rule_id;

---------------------------------------dwt_area_topic----------------------------------
DROP TABLE IF EXISTS dwt_area_topic;
CREATE EXTERNAL TABLE dwt_area_topic(
    `province_id` STRING COMMENT '编号',
    `visit_last_1d_count` BIGINT COMMENT '最近1日访客访问次数',
    `login_last_1d_count` BIGINT COMMENT '最近1日用户访问次数',
    `visit_last_7d_count` BIGINT COMMENT '最近7访客访问次数',
    `login_last_7d_count` BIGINT COMMENT '最近7日用户访问次数',
    `visit_last_30d_count` BIGINT COMMENT '最近30日访客访问次数',
    `login_last_30d_count` BIGINT COMMENT '最近30日用户访问次数',
    `visit_count` BIGINT COMMENT '累积访客访问次数',
    `login_count` BIGINT COMMENT '累积用户访问次数',
    `order_last_1d_count` BIGINT COMMENT '最近1天下单次数',
    `order_last_1d_original_amount` DECIMAL(16,2) COMMENT '最近1天下单原始金额',
    `order_last_1d_final_amount` DECIMAL(16,2) COMMENT '最近1天下单最终金额',
    `order_last_7d_count` BIGINT COMMENT '最近7天下单次数',
    `order_last_7d_original_amount` DECIMAL(16,2) COMMENT '最近7天下单原始金额',
    `order_last_7d_final_amount` DECIMAL(16,2) COMMENT '最近7天下单最终金额',
    `order_last_30d_count` BIGINT COMMENT '最近30天下单次数',
    `order_last_30d_original_amount` DECIMAL(16,2) COMMENT '最近30天下单原始金额',
    `order_last_30d_final_amount` DECIMAL(16,2) COMMENT '最近30天下单最终金额',
    `order_count` BIGINT COMMENT '累积下单次数',
    `order_original_amount` DECIMAL(16,2) COMMENT '累积下单原始金额',
    `order_final_amount` DECIMAL(16,2) COMMENT '累积下单最终金额',
    `payment_last_1d_count` BIGINT COMMENT '最近1天支付次数',
    `payment_last_1d_amount` DECIMAL(16,2) COMMENT '最近1天支付金额',
    `payment_last_7d_count` BIGINT COMMENT '最近7天支付次数',
    `payment_last_7d_amount` DECIMAL(16,2) COMMENT '最近7天支付金额',
    `payment_last_30d_count` BIGINT COMMENT '最近30天支付次数',
    `payment_last_30d_amount` DECIMAL(16,2) COMMENT '最近30天支付金额',
    `payment_count` BIGINT COMMENT '累积支付次数',
    `payment_amount` DECIMAL(16,2) COMMENT '累积支付金额',
    `refund_order_last_1d_count` BIGINT COMMENT '最近1天退单次数',
    `refund_order_last_1d_amount` DECIMAL(16,2) COMMENT '最近1天退单金额',
    `refund_order_last_7d_count` BIGINT COMMENT '最近7天退单次数',
    `refund_order_last_7d_amount` DECIMAL(16,2) COMMENT '最近7天退单金额',
    `refund_order_last_30d_count` BIGINT COMMENT '最近30天退单次数',
    `refund_order_last_30d_amount` DECIMAL(16,2) COMMENT '最近30天退单金额',
    `refund_order_count` BIGINT COMMENT '累积退单次数',
    `refund_order_amount` DECIMAL(16,2) COMMENT '累积退单金额',
    `refund_payment_last_1d_count` BIGINT COMMENT '最近1天退款次数',
    `refund_payment_last_1d_amount` DECIMAL(16,2) COMMENT '最近1天退款金额',
    `refund_payment_last_7d_count` BIGINT COMMENT '最近7天退款次数',
    `refund_payment_last_7d_amount` DECIMAL(16,2) COMMENT '最近7天退款金额',
    `refund_payment_last_30d_count` BIGINT COMMENT '最近30天退款次数',
    `refund_payment_last_30d_amount` DECIMAL(16,2) COMMENT '最近30天退款金额',
    `refund_payment_count` BIGINT COMMENT '累积退款次数',
    `refund_payment_amount` DECIMAL(16,2) COMMENT '累积退款金额'
) COMMENT '地区主题宽表'
PARTITIONED BY (`dt` STRING)
STORED AS PARQUET
LOCATION '/warehouse/gmall/dwt/dwt_area_topic/'
TBLPROPERTIES ("parquet.compression"="lzo");

insert overwrite table dwt_area_topic partition(dt='2020-06-14')
select
    id,
    nvl(visit_last_1d_count,0),
    nvl(login_last_1d_count,0),
    nvl(visit_last_7d_count,0),
    nvl(login_last_7d_count,0),
    nvl(visit_last_30d_count,0),
    nvl(login_last_30d_count,0),
    nvl(visit_count,0),
    nvl(login_count,0),
    nvl(order_last_1d_count,0),
    nvl(order_last_1d_original_amount,0),
    nvl(order_last_1d_final_amount,0),
    nvl(order_last_7d_count,0),
    nvl(order_last_7d_original_amount,0),
    nvl(order_last_7d_final_amount,0),
    nvl(order_last_30d_count,0),
    nvl(order_last_30d_original_amount,0),
    nvl(order_last_30d_final_amount,0),
    nvl(order_count,0),
    nvl(order_original_amount,0),
    nvl(order_final_amount,0),
    nvl(payment_last_1d_count,0),
    nvl(payment_last_1d_amount,0),
    nvl(payment_last_7d_count,0),
    nvl(payment_last_7d_amount,0),
    nvl(payment_last_30d_count,0),
    nvl(payment_last_30d_amount,0),
    nvl(payment_count,0),
    nvl(payment_amount,0),
    nvl(refund_order_last_1d_count,0),
    nvl(refund_order_last_1d_amount,0),
    nvl(refund_order_last_7d_count,0),
    nvl(refund_order_last_7d_amount,0),
    nvl(refund_order_last_30d_count,0),
    nvl(refund_order_last_30d_amount,0),
    nvl(refund_order_count,0),
    nvl(refund_order_amount,0),
    nvl(refund_payment_last_1d_count,0),
    nvl(refund_payment_last_1d_amount,0),
    nvl(refund_payment_last_7d_count,0),
    nvl(refund_payment_last_7d_amount,0),
    nvl(refund_payment_last_30d_count,0),
    nvl(refund_payment_last_30d_amount,0),
    nvl(refund_payment_count,0),
    nvl(refund_payment_amount,0)
from
(
    select
        id
    from dim_base_province
)t1
left join
(
    select
        province_id province_id,
        sum(if(dt='2020-06-14',visit_count,0)) visit_last_1d_count,
        sum(if(dt='2020-06-14',login_count,0)) login_last_1d_count,
        sum(if(dt>=date_add('2020-06-14',-6),visit_count,0)) visit_last_7d_count,
        sum(if(dt>=date_add('2020-06-14',-6),login_count,0)) login_last_7d_count,
        sum(if(dt>=date_add('2020-06-14',-29),visit_count,0)) visit_last_30d_count,
        sum(if(dt>=date_add('2020-06-14',-29),login_count,0)) login_last_30d_count,
        sum(visit_count) visit_count,
        sum(login_count) login_count,
        sum(if(dt='2020-06-14',order_count,0)) order_last_1d_count,
        sum(if(dt='2020-06-14',order_original_amount,0)) order_last_1d_original_amount,
        sum(if(dt='2020-06-14',order_final_amount,0)) order_last_1d_final_amount,
        sum(if(dt>=date_add('2020-06-14',-6),order_count,0)) order_last_7d_count,
        sum(if(dt>=date_add('2020-06-14',-6),order_original_amount,0)) order_last_7d_original_amount,
        sum(if(dt>=date_add('2020-06-14',-6),order_final_amount,0)) order_last_7d_final_amount,
        sum(if(dt>=date_add('2020-06-14',-29),order_count,0)) order_last_30d_count,
        sum(if(dt>=date_add('2020-06-14',-29),order_original_amount,0)) order_last_30d_original_amount,
        sum(if(dt>=date_add('2020-06-14',-29),order_final_amount,0)) order_last_30d_final_amount,
        sum(order_count) order_count,
        sum(order_original_amount) order_original_amount,
        sum(order_final_amount) order_final_amount,
        sum(if(dt='2020-06-14',payment_count,0)) payment_last_1d_count,
        sum(if(dt='2020-06-14',payment_amount,0)) payment_last_1d_amount,
        sum(if(dt>=date_add('2020-06-14',-6),payment_count,0)) payment_last_7d_count,
        sum(if(dt>=date_add('2020-06-14',-6),payment_amount,0)) payment_last_7d_amount,
        sum(if(dt>=date_add('2020-06-14',-29),payment_count,0)) payment_last_30d_count,
        sum(if(dt>=date_add('2020-06-14',-29),payment_amount,0)) payment_last_30d_amount,
        sum(payment_count) payment_count,
        sum(payment_amount) payment_amount,
        sum(if(dt='2020-06-14',refund_order_count,0)) refund_order_last_1d_count,
        sum(if(dt='2020-06-14',refund_order_amount,0)) refund_order_last_1d_amount,
        sum(if(dt>=date_add('2020-06-14',-6),refund_order_count,0)) refund_order_last_7d_count,
        sum(if(dt>=date_add('2020-06-14',-6),refund_order_amount,0)) refund_order_last_7d_amount,
        sum(if(dt>=date_add('2020-06-14',-29),refund_order_count,0)) refund_order_last_30d_count,
        sum(if(dt>=date_add('2020-06-14',-29),refund_order_amount,0)) refund_order_last_30d_amount,
        sum(refund_order_count) refund_order_count,
        sum(refund_order_amount) refund_order_amount,
        sum(if(dt='2020-06-14',refund_payment_count,0)) refund_payment_last_1d_count,
        sum(if(dt='2020-06-14',refund_payment_amount,0)) refund_payment_last_1d_amount,
        sum(if(dt>=date_add('2020-06-14',-6),refund_payment_count,0)) refund_payment_last_7d_count,
        sum(if(dt>=date_add('2020-06-14',-6),refund_payment_amount,0)) refund_payment_last_7d_amount,
        sum(if(dt>=date_add('2020-06-14',-29),refund_payment_count,0)) refund_payment_last_30d_count,
        sum(if(dt>=date_add('2020-06-14',-29),refund_payment_amount,0)) refund_payment_last_30d_amount,
        sum(refund_payment_count) refund_payment_count,
        sum(refund_payment_amount) refund_payment_amount
    from dws_area_stats_daycount
    group by province_id
)t2
on t1.id=t2.province_id;

---------------------------------------ads_visit_stats----------------------------------
DROP TABLE IF EXISTS ads_visit_stats;
CREATE EXTERNAL TABLE ads_visit_stats (
  `dt` STRING COMMENT '统计日期',
  `is_new` STRING COMMENT '新老标识,1:新,0:老',
  `recent_days` BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
  `channel` STRING COMMENT '渠道',
  `uv_count` BIGINT COMMENT '日活(访问人数)',
  `duration_sec` BIGINT COMMENT '页面停留总时长',
  `avg_duration_sec` BIGINT COMMENT '一次会话，页面停留平均时长,单位为秒',
  `page_count` BIGINT COMMENT '页面总浏览数',
  `avg_page_count` BIGINT COMMENT '一次会话，页面平均浏览数',
  `sv_count` BIGINT COMMENT '会话次数',
  `bounce_count` BIGINT COMMENT '跳出数',
  `bounce_rate` DECIMAL(16,2) COMMENT '跳出率'
) COMMENT '访客统计'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ads/ads_visit_stats/';

insert overwrite table ads_visit_stats
select * from ads_visit_stats
union
select
    '2020-06-14' dt,
    is_new,
    recent_days,
    channel,
    count(distinct(mid_id)) uv_count,
    cast(sum(during_time)/1000 as bigint) duration_sec,
    cast(avg(during_time)/1000 as bigint) avg_duration_sec,
    sum(page_count),
    cast(avg(page_count) as bigint) avg_page_count,
    count(*) sv_count,
    sum(if(page_count=1,1,0)) bounce_count,
    cast(sum(if(page_count=1,1,0))/count(*)*100 as decimal(16,2)) bounce_rate
from
(
    select
        mid_id,
        is_new,
        recent_days,
        channel,
        session_id,
        count(*) page_count,
        sum(during_time) during_time
    from
    (
        select
            t1.mid_id,
            if(visit_date_first>=date_add('2020-06-14',-recent_days+1),'1','0') is_new,
            recent_days,
            channel,
            last_page_id,
            page_id,
            ts,
            during_time,
            session_start_point,
            concat(t1.mid_id,'-',last_value(session_start_point,true) over(partition by t1.mid_id,recent_days order by ts)) session_id
        from
        (
            select
                mid_id,
                channel,
                recent_days,
                last_page_id,
                page_id,
                ts,
                during_time,
                if(last_page_id is null,ts,null) session_start_point
            from dwd_page_log
            lateral view explode(array(1,7,30)) tmp as recent_days
            where dt>=date_add('2020-06-14',-29)
            and dt >= date_add('2020-06-14',-recent_days + 1)
        ) t1
        left join
        (
            select
                mid_id,
                visit_date_first
            from dwt_visitor_topic
            where dt='2020-06-14'
        ) t0  --为了找出真正的is_new字段
        on t1.mid_id = t0.mid_id
    ) t2
    group by is_new,recent_days,channel,mid_id,session_id
) t3
group by is_new,recent_days,channel;

---------------------------------------ads_page_path----------------------------------
DROP TABLE IF EXISTS ads_page_path;
CREATE EXTERNAL TABLE ads_page_path
(
    `dt` STRING COMMENT '统计日期',
    `recent_days` BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `source` STRING COMMENT '跳转起始页面ID',
    `target` STRING COMMENT '跳转终到页面ID',
    `path_count` BIGINT COMMENT '跳转次数'
)  COMMENT '页面浏览路径'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ads/ads_page_path/';

insert overwrite table ads_page_path
select * from ads_page_path
union
select
    '2020-06-14',
    recent_days,
    source,
    target,
    count(*)
from
(
    select
        recent_days,
        concat('step-',step,':',source) source,
        concat('step-',step+1,':',target) target
    from
    (
        select
            recent_days,
            page_id source,
            lead(page_id,1,null) over (partition by recent_days,session_id order by ts) target,
            row_number() over (partition by recent_days,session_id order by ts) step
        from
        (
            select
                recent_days,
                last_page_id,
                page_id,
                ts,
                concat(mid_id,'-',last_value(if(last_page_id is null,ts,null),true) over (partition by mid_id,recent_days order by ts)) session_id
            from dwd_page_log lateral view explode(Array(1,7,30)) tmp as recent_days
            where dt>=date_add('2020-06-14',-29)
            and dt>=date_add('2020-06-14',-recent_days+1)
        )t2
    )t3
)t4
group by recent_days,source,target;

---------------------------------------ads_user_total----------------------------------
DROP TABLE IF EXISTS ads_user_total;
CREATE EXTERNAL TABLE ads_user_total(
  `dt` STRING COMMENT '统计日期',
  `recent_days` BIGINT COMMENT '最近天数,0:累积值,1:最近1天,7:最近7天,30:最近30天',
  `new_user_count` BIGINT COMMENT '新注册用户数',
  `new_order_user_count` BIGINT COMMENT '新增下单用户数',
  `order_final_amount` DECIMAL(16,2) COMMENT '下单总金额',
  `order_user_count` BIGINT COMMENT '下单用户数',
  `no_order_user_count` BIGINT COMMENT '未下单用户数(具体指活跃用户中未下单用户)'
) COMMENT '用户统计'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ads/ads_user_total/';

insert overwrite table ads_user_total
select * from ads_user_total
union
select
    '2020-06-14',
    recent_days,
    sum(if(login_date_first>=recent_days_ago,1,0)) new_user_count,
    sum(if(order_date_first>=recent_days_ago,1,0)) new_order_user_count,
    sum(order_final_amount) order_final_amount,
    sum(if(order_final_amount>0,1,0)) order_user_count,
    sum(if(order_final_amount=0 and login_date_last>=recent_days_ago,1,0)) no_order_user_count
from
(
    select
        recent_days,
        login_date_first,
        login_date_last,
        order_date_first,
        case
            when recent_days=1 then order_last_1d_final_amount
            when recent_days=7 then order_last_7d_final_amount
            when recent_days=30 then order_last_30d_final_amount
            when recent_days=0 then order_final_amount
        end order_final_amount,
        if(recent_days=0,'1970-01-01',date_add('2020-06-14',-recent_days+1)) recent_days_ago
    from dwt_user_topic
    lateral view explode(`array`(0,1,7,30)) tmp as recent_days
    where dt='2020-06-14'
) t1
group by recent_days;

---------------------------------------ads_user_change----------------------------------
DROP TABLE IF EXISTS ads_user_change;
CREATE EXTERNAL TABLE ads_user_change(
  `dt` STRING COMMENT '统计日期',
  `user_churn_count` BIGINT COMMENT '流失用户数(新增)',
  `user_back_count` BIGINT COMMENT '回流用户数'
) COMMENT '用户变动统计'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ads/ads_user_change/';

insert overwrite table ads_user_change
select * from ads_user_change
union
select
    churn.dt,
    user_churn_count,
    user_back_count
from
(
    select
        '2020-06-14' dt,
        count(*) user_churn_count
    from dwt_user_topic
    where dt='2020-06-14'
    and login_date_last=date_add('2020-06-14',-7)
)churn
join
(
    select
        '2020-06-14' dt,
        count(*) user_back_count
    from
    (
        select
            user_id
        from dwt_user_topic
        where dt='2020-06-14'
        and login_date_last='2020-06-14'
    )t1  --今日活跃用户
    join
    (
        select
            user_id
        from dwt_user_topic
        where dt=date_add('2020-06-14',-1)
        and login_date_last<=date_add('2020-06-14',-8)
    )t2  --前七日流失用户
    on t1.user_id=t2.user_id
)back
on churn.dt=back.dt;

---------------------------------------ads_user_action----------------------------------
DROP TABLE IF EXISTS ads_user_action;
CREATE EXTERNAL TABLE ads_user_action(
  `dt` STRING COMMENT '统计日期',
  `recent_days` BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
  `home_count` BIGINT COMMENT '浏览首页人数',
  `good_detail_count` BIGINT COMMENT '浏览商品详情页人数',
  `cart_count` BIGINT COMMENT '加入购物车人数',
  `order_count` BIGINT COMMENT '下单人数',
  `payment_count` BIGINT COMMENT '支付人数'
) COMMENT '漏斗分析'
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ads/ads_user_action/';

with
tmp_page as
(
    select
        '2020-06-14' dt,
        recent_days,
        sum(if(page_id='home',1,0)) home_count,
        sum(if(page_id='good_detail',1,0)) good_detail_count
    from
    (
        select
            recent_days,
            mid_id,
            page_id
        from dwd_page_log
        lateral view explode(array(1,7,30)) tmp as recent_days
        where dt>=date_add('2020-06-14',-29)
        and dt>=date_add('2020-06-14',-recent_days+1)
        and page_id in ('home','good_detail')
        group by recent_days,mid_id,page_id
    )t1
    group by recent_days
),
tmp_cop as
(
    select
        '2020-06-14' dt,
        recent_days,
        sum(if(cart_count>0,1,0)) cart_count,
        sum(if(order_count>0,1,0)) order_count,
        sum(if(payment_count>0,1,0)) payment_count
    from
    (
        select
            recent_days,
            case
                when recent_days=1 then cart_last_1d_count
                when recent_days=7 then cart_last_7d_count
                when recent_days=30 then cart_last_30d_count
            end cart_count,
            case
                when recent_days=1 then order_last_1d_count
                when recent_days=7 then order_last_7d_count
                when recent_days=30 then order_last_30d_count
            end order_count,
            case
                when recent_days=1 then payment_last_1d_count
                when recent_days=7 then payment_last_7d_count
                when recent_days=30 then payment_last_30d_count
            end payment_count
        from dwt_user_topic
        lateral view explode(array(1,7,30)) tmp as recent_days
        where dt='2020-06-14'
    ) t1
    group by recent_days
)
insert overwrite table ads_user_action
select * from ads_user_action
union
select
    tmp_page.dt,
    tmp_page.recent_days,
    tmp_page.home_count,
    tmp_page.good_detail_count,
    tmp_cop.cart_count,
    tmp_cop.order_count,
    tmp_cop.payment_count
from tmp_page join tmp_cop
on tmp_page.dt=tmp_cop.dt
and tmp_page.recent_days = tmp_cop.recent_days;

---------------------------------------ads_user_retention----------------------------------
DROP TABLE IF EXISTS ads_user_retention;
CREATE EXTERNAL TABLE ads_user_retention (
  `dt` STRING COMMENT '统计日期',
  `create_date` STRING COMMENT '用户新增日期',
  `retention_day` BIGINT COMMENT '截至当前日期留存天数',
  `retention_count` BIGINT COMMENT '留存用户数量',
  `new_user_count` BIGINT COMMENT '新增用户数量',
  `retention_rate` DECIMAL(16,2) COMMENT '留存率'
) COMMENT '用户留存率'
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ads/ads_user_retention/';

insert overwrite table ads_user_retention
select * from ads_user_retention
union
select
    '2020-06-14',
    login_date_first create_date,
    datediff('2020-06-14',login_date_first) retention_day,
    sum(if(login_date_last='2020-06-14',1,0)) retention_count,
    count(*) new_user_count,
    cast(sum(if(login_date_last='2020-06-14',1,0))/count(*)*100 as decimal(16,2)) retention_rate
from dwt_user_topic
where dt='2020-06-14'
and login_date_first>=date_add('2020-06-14',-7)
and login_date_first<'2020-06-14'
group by login_date_first;

---------------------------------------ads_order_spu_stats----------------------------------
DROP TABLE IF EXISTS ads_order_spu_stats;
CREATE EXTERNAL TABLE ads_order_spu_stats(
    `dt` STRING COMMENT '统计日期',
    `recent_days` BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `spu_id` STRING COMMENT '商品ID',
    `spu_name` STRING COMMENT '商品名称',
    `tm_id` STRING COMMENT '品牌ID',
    `tm_name` STRING COMMENT '品牌名称',
    `category3_id` STRING COMMENT '三级品类ID',
    `category3_name` STRING COMMENT '三级品类名称',
    `category2_id` STRING COMMENT '二级品类ID',
    `category2_name` STRING COMMENT '二级品类名称',
    `category1_id` STRING COMMENT '一级品类ID',
    `category1_name` STRING COMMENT '一级品类名称',
    `order_count` BIGINT COMMENT '订单数',
    `order_amount` DECIMAL(16,2) COMMENT '订单金额'
) COMMENT '商品销售统计'
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ads/ads_order_spu_stats/';

insert overwrite table ads_order_spu_stats
select * from ads_order_spu_stats
union
select
    '2020-06-14' dt,
    recent_days,
    spu_id,
    spu_name,
    tm_id,
    tm_name,
    category3_id,
    category3_name,
    category2_id,
    category2_name,
    category1_id,
    category1_name,
    sum(order_count),
    sum(order_amount)
from
(
    select
        recent_days,
        sku_id,
        case
            when recent_days=1 then order_last_1d_count
            when recent_days=7 then order_last_7d_count
            when recent_days=30 then order_last_30d_count
        end order_count,
        case
            when recent_days=1 then order_last_1d_final_amount
            when recent_days=7 then order_last_7d_final_amount
            when recent_days=30 then order_last_30d_final_amount
        end order_amount
from dwt_sku_topic
lateral view explode(Array(1,7,30)) tmp as recent_days
    where dt='2020-06-14'
)t1
left join
(
    select
        id,
        spu_id,
        spu_name,
        tm_id,
        tm_name,
        category3_id,
        category3_name,
        category2_id,
        category2_name,
        category1_id,
        category1_name
    from dim_sku_info
    where dt='2020-06-14'
)t2
on t1.sku_id=t2.id
group by recent_days,spu_id,spu_name,tm_id,tm_name,category3_id,category3_name,category2_id,category2_name,category1_id,category1_name;

---------------------------------------ads_repeat_purchase----------------------------------
DROP TABLE IF EXISTS ads_repeat_purchase;
CREATE EXTERNAL TABLE ads_repeat_purchase(
  `dt` STRING COMMENT '统计日期',
  `recent_days` BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
  `tm_id` STRING COMMENT '品牌ID',
  `tm_name` STRING COMMENT '品牌名称',
  `order_repeat_rate` DECIMAL(16,2) COMMENT '复购率'
) COMMENT '品牌复购率'
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ads/ads_repeat_purchase/';

insert overwrite table ads_repeat_purchase
select * from ads_repeat_purchase
union
select
    '2020-06-14' dt,
    recent_days,
    tm_id,
    tm_name,
    cast(sum(if(order_tm_count>=2,1,0))/sum(if(order_tm_count>=1,1,0))*100 as decimal(16,2)) order_repeat_rate
from
(
    select
        recent_days,
        user_id,
        tm_id,
        tm_name,
        sum(order_sku_count) order_tm_count
    from
    (
        select
            recent_days,
            user_id,
            sku_id,
            count(*) order_sku_count
        from dwd_order_detail
        lateral view explode(Array(1,7,30)) tmp as recent_days
        where dt>=date_add('2020-06-14',-29)
        and dt>=date_add('2020-06-14',-recent_days+1)
        group by recent_days,user_id,sku_id
    )t1
    left join
    (
        select
            id,
            tm_id,
            tm_name
        from dim_sku_info
        where dt='2020-06-14'
    )t2
    on t1.sku_id=t2.id
    group by recent_days,user_id,tm_id,tm_name
)t3
group by recent_days,tm_id,tm_name;

---------------------------------------ads_order_total----------------------------------
DROP TABLE IF EXISTS ads_order_total;
CREATE EXTERNAL TABLE ads_order_total(
  `dt` STRING COMMENT '统计日期',
  `recent_days` BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
  `order_count` BIGINT COMMENT '订单数',
  `order_amount` DECIMAL(16,2) COMMENT '订单金额',
  `order_user_count` BIGINT COMMENT '下单人数'
) COMMENT '订单统计'
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ads/ads_order_total/';

insert overwrite table ads_order_total
select * from ads_order_total
union
select
    '2020-06-14',
    recent_days,
    sum(order_count) order_count,
    sum(order_final_amount) order_final_amount,
    sum(if(order_count>0,1,0)) order_user_count
from
(
    select
        recent_days,
        user_id,
        case
            when recent_days=0 then order_count
            when recent_days=1 then order_last_1d_count
            when recent_days=7 then order_last_7d_count
            when recent_days=30 then order_last_30d_count
        end order_count,
        case
            when recent_days=0 then order_final_amount
            when recent_days=1 then order_last_1d_final_amount
            when recent_days=7 then order_last_7d_final_amount
            when recent_days=30 then order_last_30d_final_amount
        end order_final_amount
    from dwt_user_topic
    lateral view explode(Array(1,7,30)) tmp as recent_days
    where dt='2020-06-14'
)t1
group by recent_days;

---------------------------------------ads_order_by_province----------------------------------
DROP TABLE IF EXISTS ads_order_by_province;
CREATE EXTERNAL TABLE ads_order_by_province(
  `dt` STRING COMMENT '统计日期',
  `recent_days` BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
  `province_id` STRING COMMENT '省份id',
  `province_name` STRING COMMENT '省份名称',
  `area_code` STRING COMMENT '地区编码',
  `iso_code` STRING COMMENT '国际标准地区编码',
  `iso_code_3166_2` STRING COMMENT '国际标准地区编码',
  `order_count` BIGINT COMMENT '订单数',
  `order_amount` DECIMAL(16,2) COMMENT '订单金额'
) COMMENT '各省份订单统计'
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ads/ads_order_by_province/';

insert overwrite table ads_order_by_province
select * from ads_order_by_province
union
select
    '2020-06-14' dt,
    t1.recent_days,
    t1.province_id,
    t2.province_name,
    t2.area_code,
    t2.iso_code,
    t2.iso_3166_2,
    t1.order_count,
    t1.order_amount
from
(
    select
        recent_days,
        province_id,
        case
            when recent_days=1 then order_last_1d_count
            when recent_days=7 then order_last_7d_count
            when recent_days=30 then order_last_30d_count
        end order_count,
        case
            when recent_days=1 then order_last_1d_final_amount
            when recent_days=7 then order_last_7d_final_amount
            when recent_days=30 then order_last_30d_final_amount
        end order_amount
    from dwt_area_topic
    lateral view explode(Array(1,7,30)) tmp as recent_days
    where dt='2020-06-14'
)t1
left join
(
    select
        id,
        province_name,
        area_code,
        iso_code,
        iso_3166_2
    from dim_base_province
)t2
on t1.province_id=t2.id;

show tables;


