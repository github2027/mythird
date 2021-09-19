set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

DROP TABLE IF EXISTS dwd_cart_info;
CREATE EXTERNAL TABLE dwd_cart_info
(
    `id`           STRING COMMENT '编号',
    `user_id`      STRING COMMENT '用户ID',
    `sku_id`       STRING COMMENT '商品ID',
    `source_type`  STRING COMMENT '来源类型',
    `source_id`    STRING COMMENT '来源编号',
    `cart_price`   DECIMAL(16, 2) COMMENT '加入购物车时的价格',
    `is_ordered`   STRING COMMENT '是否已下单',
    `create_time`  STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间',
    `order_time`   STRING COMMENT '下单时间',
    `sku_num`      BIGINT COMMENT '加购数量'
) COMMENT '加购事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall/dwd/dwd_cart_info/'
    TBLPROPERTIES ("parquet.compression" = "lzo");

insert into table dwd_cart_info partition (dt = '2020-06*14')
select id,
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
where dt = '2020-06-14';

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

insert into table dwd_favor_info partition (dt = '2020-06-14')
select id,
       user_id,
       sku_id,
       spu_id,
       is_cancel,
       create_time,
       cancel_time
from ods_favor_info
where dt = '2020-06-14';

DROP TABLE IF EXISTS dwd_coupon_use;
CREATE EXTERNAL TABLE dwd_coupon_use
(
    `id`            STRING COMMENT '编号',
    `coupon_id`     STRING COMMENT '优惠券ID',
    `user_id`       STRING COMMENT 'userid',
    `order_id`      STRING COMMENT '订单id',
    `coupon_status` STRING COMMENT '优惠券状态',
    `get_time`      STRING COMMENT '领取时间',
    `using_time`    STRING COMMENT '使用时间(下单)',
    `used_time`     STRING COMMENT '使用时间(支付)',
    `expire_time`   STRING COMMENT '过期时间'
) COMMENT '优惠券领用事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall/dwd/dwd_coupon_use/'
    TBLPROPERTIES ("parquet.compression" = "lzo");

insert into table dwd_coupon_use partition (dt)
select id,
       coupon_id,
       user_id,
       order_id,
       coupon_status,
       get_time,
       using_time,
       used_time,
       expire_time,
       coalesce(date_format(used_time, 'yyyy-MM-dd'), date_format(expire_time, 'yyyy-MM-dd'), '9999-99-99')
from ods_coupon_use
where dt = '2020-06-14';

insert overwrite table dwd_coupon_use partition (dt)
select nvl(new.id, old.id),
       nvl(new.coupon_id, old.coupon_id),
       nvl(new.user_id, old.user_id),
       nvl(new.order_id, old.order_id),
       nvl(new.coupon_status, old.coupon_status),
       nvl(new.get_time, old.get_time),
       nvl(new.using_time, old.using_time),
       nvl(new.used_time, old.used_time),
       nvl(new.expire_time, old.expire_time),
       // coalesce(date_format(nvl(new.expire_time,old.expire_time),'yyyy-MM-dd'),date_format(nvl(new.used_time,old.used_time),'yyyy-MM-dd'),'9999-99-99')
       coalesce(date_format(new.expire_time, 'yyyy-MM-dd'), date_format(new.used_time, 'yyyy-MM-dd'), '9999-99-99')
from (select id,
             coupon_id,
             user_id,
             order_id,
             coupon_status,
             get_time,
             using_time,
             used_time,
             expire_time
      from ods_coupon_use
      where dt = '2020-06-15') new
         full OUTER JOIN
     (select id,
             coupon_id,
             user_id,
             order_id,
             coupon_status,
             get_time,
             using_time,
             used_time,
             expire_time
      from dwd_coupon_use
      where dt = '9999-99-99') old

where old.id = new.id;

DROP TABLE IF EXISTS dwd_payment_info;
CREATE EXTERNAL TABLE dwd_payment_info
(
    `id`             STRING COMMENT '编号',
    `order_id`       STRING COMMENT '订单编号',
    `user_id`        STRING COMMENT '用户编号',
    `province_id`    STRING COMMENT '地区ID',
    `trade_no`       STRING COMMENT '交易编号',
    `out_trade_no`   STRING COMMENT '对外交易编号',
    `payment_type`   STRING COMMENT '支付类型',
    `payment_amount` DECIMAL(16, 2) COMMENT '支付金额',
    `payment_status` STRING COMMENT '支付状态',
    `create_time`    STRING COMMENT '创建时间',--调用第三方支付接口的时间
    `callback_time`  STRING COMMENT '完成时间'--支付完成时间，即支付成功回调时间
) COMMENT '支付事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall/dwd/dwd_payment_info/'
    TBLPROPERTIES ("parquet.compression" = "lzo");

insert overwrite table dwd_payment_info partition (dt)
select pa.id,
       order_id,
       user_id,
       province_id,
       trade_no,
       out_trade_no,
       payment_type,
       payment_amount,
       payment_status,
       create_time,
       callback_time,
       nvl(date_format(callback_time, 'yyyy-MM-dd'), '9999-99-99')
from (select id,
             out_trade_no,
             order_id,
             user_id,
             payment_type,
             trade_no,
             payment_amount,
             subject,
             payment_status,
             create_time,
             callback_time
      from ods_payment_info
      where dt = '2020-06-14') pa
         left join
     (select id,
             province_id
      from ods_order_info
      where dt = '2020-06-14') oi
     on pa.id = oi.id;

DROP TABLE IF EXISTS dwd_refund_payment;
CREATE EXTERNAL TABLE dwd_refund_payment
(
    `id`            STRING COMMENT '编号',
    `user_id`       STRING COMMENT '用户ID',
    `order_id`      STRING COMMENT '订单编号',
    `sku_id`        STRING COMMENT 'SKU编号',
    `province_id`   STRING COMMENT '地区ID',
    `trade_no`      STRING COMMENT '交易编号',
    `out_trade_no`  STRING COMMENT '对外交易编号',
    `payment_type`  STRING COMMENT '支付类型',
    `refund_amount` DECIMAL(16, 2) COMMENT '退款金额',
    `refund_status` STRING COMMENT '退款状态',
    `create_time`   STRING COMMENT '创建时间',--调用第三方支付接口的时间
    `callback_time` STRING COMMENT '回调时间'--支付接口回调时间，即支付成功时间
) COMMENT '退款事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall/dwd/dwd_refund_payment/'
    TBLPROPERTIES ("parquet.compression" = "lzo");

insert overwrite table dwd_refund_payment partition (dt)
select re.id,
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
       nvl(date_format(callback_time, 'yyyy-MM-dd'), '9999-99-99')
from (select id,
             order_id,
             sku_id,
             trade_no,
             out_trade_no,
             payment_type,
             refund_amount,
             refund_status,
             create_time,
             callback_time
      from ods_refund_payment
      where dt = '2020-06-14') re
         left join
     (select id,
             user_id,
             province_id
      from ods_order_info
      where dt = '2020-06-14') ord
     on re.id = ord.id;

insert overwrite table dwd_refund_payment partition (dt)

select nvl(new.id, old.id),
       nvl(new.user_id, old.user_id),
       nvl(new.order_id, old.order_id),
       nvl(new.sku_id, old.sku_id),
       nvl(new.province_id, old.province_id),
       nvl(new.trade_no, old.trade_no),
       nvl(new.out_trade_no, old.out_trade_no),
       nvl(new.payment_type, old.payment_type),
       nvl(new.refund_amount, old.refund_amount),
       nvl(new.refund_status, old.refund_status),
       nvl(new.create_time, old.create_time),
       nvl(new.callback_time, old.callback_time),
       nvl(date_format(nvl(new.callback_time, old.callback_time), 'yyyy-MM-dd'), '9999-99-99')
from (select id,
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
      where dt = '9999-99-99') old
         full join
     (
         (select orp.id,
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
          from (select id,
                       out_trade_no,
                       order_id,
                       sku_id,
                       payment_type,
                       trade_no,
                       refund_amount,
                       subject,
                       refund_status,
                       create_time,
                       callback_time
                from ods_refund_payment
                where dt = '2020-06-15') orp
                   left join
               (select id,
                       user_id,
                       province_id
                from ods_order_info
                where dt = '2020-06-15') ooi
               on orp.id = ooi.id)
     ) new
     on old.id = new.id;

insert overwrite table dwd_refund_payment partition (dt)
select nvl(new.id, old.id),
       nvl(new.user_id, old.user_id),
       nvl(new.order_id, old.order_id),
       nvl(new.sku_id, old.sku_id),
       nvl(new.province_id, old.province_id),
       nvl(new.trade_no, old.trade_no),
       nvl(new.out_trade_no, old.out_trade_no),
       nvl(new.payment_type, old.payment_type),
       nvl(new.refund_amount, old.refund_amount),
       nvl(new.refund_status, old.refund_status),
       nvl(new.create_time, old.create_time),
       nvl(new.callback_time, old.callback_time),
       nvl(date_format(nvl(new.callback_time, old.callback_time), 'yyyy-MM-dd'), '9999-99-99')
from (
         select id,
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
         where dt = '9999-99-99'
     ) old
         full outer join
     (
         select rp.id,
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
         from (
                  select id,
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
                  where dt = '2020-06-15'
              ) rp
                  left join
              (
                  select id,
                         user_id,
                         province_id
                  from ods_order_info
                  where dt = '2020-06-15'
              ) oi
              on rp.order_id = oi.id
     ) new
     on old.id = new.id;

DROP TABLE IF EXISTS dwd_order_info;
CREATE EXTERNAL TABLE dwd_order_info
(
    `id`                     STRING COMMENT '订单号',
    `order_status`           STRING COMMENT '订单状态',
    `user_id`                STRING COMMENT '用户ID',
    `province_id`            STRING COMMENT '地区ID',
    `payment_way`            STRING COMMENT '支付方式',
    `delivery_address`       STRING COMMENT '邮寄地址',
    `out_trade_no`           STRING COMMENT '对外交易编号',
    `tracking_no`            STRING COMMENT '物流单号',
    `create_time`            STRING COMMENT '创建时间(未支付状态)',
    `payment_time`           STRING COMMENT '支付时间(已支付状态)',
    `cancel_time`            STRING COMMENT '取消时间(已取消状态)',
    `finish_time`            STRING COMMENT '完成时间(已完成状态)',
    `refund_time`            STRING COMMENT '退款时间(退款中状态)',
    `refund_finish_time`     STRING COMMENT '退款完成时间(退款完成状态)',
    `expire_time`            STRING COMMENT '过期时间',
    `feight_fee`             DECIMAL(16, 2) COMMENT '运费',
    `feight_fee_reduce`      DECIMAL(16, 2) COMMENT '运费减免',
    `activity_reduce_amount` DECIMAL(16, 2) COMMENT '活动减免',
    `coupon_reduce_amount`   DECIMAL(16, 2) COMMENT '优惠券减免',
    `original_amount`        DECIMAL(16, 2) COMMENT '订单原始价格',
    `final_amount`           DECIMAL(16, 2) COMMENT '订单最终价格'
) COMMENT '订单事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall/dwd/dwd_order_info/'
    TBLPROPERTIES ("parquet.compression" = "lzo");

insert overwrite table dwd_order_info partition (dt)

select id,
       order_status,
       user_id,
       province_id,
       payment_way,
       delivery_address,
       out_trade_no,
       tracking_no,
       create_time,
       payment_time,
       cancel_time,
       finish_time,
       refund_time,
       refund_finish_time,
       expire_time,
       feight_fee,
       feight_fee_reduce,
       activity_reduce_amount,
       coupon_reduce_amount,
       original_amount,
       final_amount,
       case
           when cancel_time is not null then date_format(cancel_time, 'yyyy-MM-dd')
           when finish_time is not null and date_add(date_format(finish_time, 'yyyy-MM-dd'), 7) >= '2020-06-14' and
                refund_time is null then date_add(date_format(finish_time, 'yyyy-MM-dd'), 7)
           when refund_finish_time is not null then date_format(refund_finish_time, 'yyyy-MM-dd')
           when expire_time is not null then date_format(expire_time, 'yyyy-MM-dd')
           else '9999-99-99'
           end
from (select id,
             order_status,
             user_id,
             province_id,
             payment_way,
             delivery_address,
             out_trade_no,
             tracking_no,
             create_time,
             ts['1002'] payment_time,
             ts['1003'] cancel_time,
             ts['1004'] finish_time,
             ts['1005'] refund_time,
             ts['1006'] refund_finish_time,
             expire_time,
             feight_fee,
             feight_fee_reduce,
             activity_reduce_amount,
             coupon_reduce_amount,
             original_amount,
             final_amount
      from (
            (select id,
                    final_amount,
                    order_status,
                    user_id,
                    payment_way,
                    delivery_address,
                    out_trade_no,
                    create_time,
                    expire_time,
                    tracking_no,
                    province_id,
                    activity_reduce_amount,
                    coupon_reduce_amount,
                    original_amount,
                    feight_fee,
                    feight_fee_reduce
             from ods_order_info
             where dt = '2020-06-14'
            ) ooi

               left join
           (select order_id,
                   str_to_map(concat_ws(',', collect_set(concat(order_status, '=', operate_time))), ',', '=') ts
            from ods_order_status_log
            where dt = '2020-06-14'
            group by order_id
           ) oosl
           on ooi.id = oosl.order_id)) times

insert into table dwd_order_info partition (dt)

select nvl(new.id, old.id),
       nvl(new.order_status, old.order_status),
       nvl(new.user_id, old.user_id),
       nvl(new.province_id, old.province_id),
       nvl(new.payment_way, old.payment_way),
       nvl(new.delivery_address, old.delivery_address),
       nvl(new.out_trade_no, old.out_trade_no),
       nvl(new.tracking_no, old.tracking_no),
       nvl(new.create_time, old.create_time),
       nvl(new.payment_time, old.payment_time),
       nvl(new.cancel_time, old.cancel_time),
       nvl(new.finish_time, old.finish_time),
       nvl(new.refund_time, old.refund_time),
       nvl(new.refund_finish_time, old.refund_finish_time),
       nvl(new.expire_time, old.expire_time),
       nvl(new.feight_fee, old.feight_fee),
       nvl(new.feight_fee_reduce, old.feight_fee_reduce),
       nvl(new.activity_reduce_amount, old.activity_reduce_amount),
       nvl(new.coupon_reduce_amount, old.coupon_reduce_amount),
       nvl(new.original_amount, old.original_amount),
       nvl(new.final_amount, old.final_amount),
       case
        when new.cancel_time is not null then date_format(new.cancel_time,'yyyy-MM-dd')
        when date_add(date_format(nvl(new.finish_time,old.finish_time),'yyyy-MM-dd'),7)='2020-06-15' and nvl(new.refund_time,old.refund_time) is null then '2020-06-15'
        when new.refund_finish_time is not null then date_format(new.refund_finish_time,'yyyy-MM-dd')
        when new.expire_time is not null then date_format(new.expire_time,'yyyy-MM-dd')
        else '9999-99-99'
      end
from
      (select id,
             order_status,
             user_id,
             province_id,
             payment_way,
             delivery_address,
             out_trade_no,
             tracking_no,
             create_time,
             payment_time,
             cancel_time,
             finish_time,
             refund_time,
             refund_finish_time,
             expire_time,
             feight_fee,
             feight_fee_reduce,
             activity_reduce_amount,
             coupon_reduce_amount,
             original_amount,
             final_amount
      from dwd_order_info
      where dt = '9999-99-99'
     ) old
         full join
    (
        select id,
                order_status,
                user_id,
                province_id,
                payment_way,
                delivery_address,
                out_trade_no,
                tracking_no,
                create_time,
                oosl.ts['1002'] payment_time,
                oosl.ts['1003'] cancel_time,
                oosl.ts['1004'] finish_time,
                oosl.ts['1005'] refund_time,
                oosl.ts['1006'] refund_finish_time,
                expire_time,
                feight_fee,
                feight_fee_reduce,
                activity_reduce_amount,
                coupon_reduce_amount,
                original_amount,
                final_amount
         from (select *
               from ods_order_info
               where dt = '2020-06-15'
              ) ooi
                  left join
              (select order_id,
                      str_to_map(concat_ws(',', collect_set(concat(order_status, '=', operate_time))), ',', '=') ts
               from ods_order_status_log
               where dt = '2020-06-15'
              ) oosl
              on ooi.id = oosl.order_id
    )new
             on old.id = new.id;


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

