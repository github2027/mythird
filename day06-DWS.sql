set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

DROP TABLE IF EXISTS dws_visitor_action_daycount;
CREATE EXTERNAL TABLE dws_visitor_action_daycount
(
    `mid_id`       STRING COMMENT '设备id',
    `brand`        STRING COMMENT '设备品牌',
    `model`        STRING COMMENT '设备型号',
    `is_new`       STRING COMMENT '是否首次访问',
    `channel`      ARRAY<STRING> COMMENT '渠道',
    `os`           ARRAY<STRING> COMMENT '操作系统',
    `area_code`    ARRAY<STRING> COMMENT '地区ID',
    `version_code` ARRAY<STRING> COMMENT '应用版本',
    `visit_count`  BIGINT COMMENT '访问次数',
    `page_stats`   ARRAY<STRUCT<page_id:STRING,page_count:BIGINT,during_time:BIGINT>> COMMENT '页面访问统计'
) COMMENT '每日设备行为表'
    PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall/dws/dws_visitor_action_daycount'
    TBLPROPERTIES ("parquet.compression" = "lzo");

DROP TABLE IF EXISTS dws_user_action_daycount;
CREATE EXTERNAL TABLE dws_user_action_daycount
(
    `user_id`                      STRING COMMENT '用户id',
    `login_count`                  BIGINT COMMENT '登录次数',
    `cart_count`                   BIGINT COMMENT '加入购物车次数',
    `favor_count`                  BIGINT COMMENT '收藏次数',
    `order_count`                  BIGINT COMMENT '下单次数',
    `order_activity_count`         BIGINT COMMENT '订单参与活动次数',
    `order_activity_reduce_amount` DECIMAL(16, 2) COMMENT '订单减免金额(活动)',
    `order_coupon_count`           BIGINT COMMENT '订单用券次数',
    `order_coupon_reduce_amount`   DECIMAL(16, 2) COMMENT '订单减免金额(优惠券)',
    `order_original_amount`        DECIMAL(16, 2) COMMENT '订单单原始金额',
    `order_final_amount`           DECIMAL(16, 2) COMMENT '订单总金额',
    `payment_count`                BIGINT COMMENT '支付次数',
    `payment_amount`               DECIMAL(16, 2) COMMENT '支付金额',
    `refund_order_count`           BIGINT COMMENT '退单次数',
    `refund_order_num`             BIGINT COMMENT '退单件数',
    `refund_order_amount`          DECIMAL(16, 2) COMMENT '退单金额',
    `refund_payment_count`         BIGINT COMMENT '退款次数',
    `refund_payment_num`           BIGINT COMMENT '退款件数',
    `refund_payment_amount`        DECIMAL(16, 2) COMMENT '退款金额',
    `coupon_get_count`             BIGINT COMMENT '优惠券领取次数',
    `coupon_using_count`           BIGINT COMMENT '优惠券使用(下单)次数',
    `coupon_used_count`            BIGINT COMMENT '优惠券使用(支付)次数',
    `appraise_good_count`          BIGINT COMMENT '好评数',
    `appraise_mid_count`           BIGINT COMMENT '中评数',
    `appraise_bad_count`           BIGINT COMMENT '差评数',
    `appraise_default_count`       BIGINT COMMENT '默认评价数',
    `order_detail_stats`           array<struct<sku_id :string,sku_num :bigint,order_count :bigint,activity_reduce_amount
                                                :decimal(16, 2),coupon_reduce_amount :decimal(16, 2),original_amount
                                                :decimal(16, 2),final_amount :decimal(16, 2)>> COMMENT '下单明细统计'
) COMMENT '每日用户行为'
    PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall/dws/dws_user_action_daycount/'
    TBLPROPERTIES ("parquet.compression" = "lzo");

select user_id,
       dt,
       count(*)
from dwd_page_log
where user_id is not null
  and last_page_id is null
group by user_id, dt;

/*select
    user_id,
       dt,
    count(*) cart_count
from dwd_action_log
where user_id is not null and
      action_id = 'cart_add'
group by user_id,dt;

select
    user_id,
       dt,
    count(*) favor_add
from dwd_action_log

where user_id is not null and
      action_id = 'favor_add'
group by user_id,dt;*/

select dt,
       user_id,
       sum(if(action_id = 'cart_add', 1, 0))  cart_count,
       sum(if(action_id = 'favor_add', 1, 0)) favor_count
from dwd_action_log
where user_id is not null
  and action_id in ('cart_add', 'favor_add')
group by user_id, dt;

select user_id,
       date_format(create_time, 'yyyy-MM-dd'),
       count(*)                                  order_count,
       sum(if(activity_reduce_amount > 0, 1, 0)) order_activity_count,
       sum(activity_reduce_amount)               order_activity_reduce_amount,
       sum(if(coupon_reduce_amount > 0, 1, 0))   order_coupon_count,
       sum(coupon_reduce_amount)                 order_coupou_reduce_amount,
       sum(original_amount)                      order_original_amount,
       sum(final_amount)                         order_final_amount
from dwd_order_info
group by user_id, date_format(create_time, 'yyyy-MM-dd')

select dt,
       user_id,
       sum(payment_amount) payment_amount
from dwd_payment_info
where dt != '9999-99-99'
group by user_id, dt;

/*select
    dt,
    user_id,
    count(*) refund_order_count,
    sum(refund_num) refund_order_num,
    sum(refund_amount) refund_order_amount
from dwd_order_refund_info
group by user_id,dt;

select
    dt,
    user_id,
    count(*) refund_payment_count,
     sum()   refund_payment_num,
      sum(refund_amount) refund_payment_amount
                from dwd_refund_payment

where dt!='9999-99-99'
group by user_id,dt;*/

select dt,
       user_id,
       count(*)           refund_payment_count,
       sum(refund_amount) refund_payment_num,
       sum(refund_amount) refund_payment_amount
from (select dt,
             user_id,
             order_id,
             sku_id
      from dwd_refund_payment
      where dt != '9999-99-99') rp

         left join

     (select order_id,
             sku_id,
             refund_amount
      from dwd_order_refund_info) ri
     on rp.order_id = ri.order_id
         and rp.sku_id = ri.sku_id
group by user_id, dt


select coalesce(coupon_get.dt, coupon_using.dt, coupon_used.dt)                dt,
       coalesce(coupon_get.user_id, coupon_using.user_id, coupon_used.user_id) user_id,
       nvl(get_count, 0),
       nvl(used_count, 0),
       nvl(using_count, 0)
from (
         select date_format(get_time, 'yyyy-MM-dd') dt,
                user_id,
                count(*)                            get_count
         from dwd_coupon_use
         where get_time is not null
         group by user_id, date_format(get_time, 'yyyy-MM-dd')
     ) coupon_get
         full join
     (
         select date_format(using_time, 'yyyy-MM-dd') dt,
                user_id,
                count(*)                              using_count
         from dwd_coupon_use
         where using_time is not null
         group by user_id, date_format(using_time, 'yyyy-MM-dd')
     ) coupon_using
     on coupon_get.dt = coupon_using.dt
         and coupon_get.user_id = coupon_using.user_id
         full outer join
     (
         select date_format(used_time, 'yyyy-MM-dd') dt,
                user_id,
                count(*)                             used_count
         from dwd_coupon_use
         where used_time is not null
         group by user_id, date_format(used_time, 'yyyy-MM-dd')
     ) coupon_used
     on nvl(coupon_get.user_id, coupon_using.user_id) = coupon_used.user_id
         and nvl(coupon_get.dt, coupon_using.dt) = coupon_used.dt

select sum(if(appraise = '1201', 1, 0)),
       sum(if(appraise = '1202', 1, 0)),
       sum(if(appraise = '1203', 1, 0)),
       sum(if(appraise = '1204', 1, 0))
from dwd_comment_info
group by user_id, dt

select dt,
       user_id,
       collect_set(named_struct('sku_id', sku_id, 'sku_num', sku_num, 'order_count',
                                order_count, 'activity_reduce_amount', activity_reduce_amount,
                                'coupon_reduce_amount', coupon_reduce_amount, 'original_amount',
                                original_amount, 'final_amount', final_amount))
from (
         select dt,
                user_id,
                sku_id,
                sum(sku_num)               sku_num,
                count(*)                   order_count,
                sum(split_activity_amount) activity_reduce_amount,
                sum(split_coupon_amount)   coupon_reduce_amount,
                sum(original_amount)       original_amount,
                sum(split_final_amount)    final_amount
         from dwd_order_detail
         group by user_id, dt, sku_id
     ) t1
group by user_id, dt;

---------------------------------------<首日装载>---------------------------------------------------
with tmp_login as
         (
             select dt,
                    user_id,
                    count(*) login_count
             from dwd_page_log
             where user_id is not null
               and last_page_id is null
             group by dt, user_id
         ),
     tmp_cf as
         (
             select dt,
                    user_id,
                    sum(if(action_id = 'cart_add', 1, 0))  cart_count,
                    sum(if(action_id = 'favor_add', 1, 0)) favor_count
             from dwd_action_log
             where user_id is not null
               and action_id in ('cart_add', 'favor_add')
             group by dt, user_id
         ),
     tmp_order as
         (
             select date_format(create_time, 'yyyy-MM-dd')    dt,
                    user_id,
                    count(*)                                  order_count,
                    sum(if(activity_reduce_amount > 0, 1, 0)) order_activity_count,
                    sum(if(coupon_reduce_amount > 0, 1, 0))   order_coupon_count,
                    sum(activity_reduce_amount)               order_activity_reduce_amount,
                    sum(coupon_reduce_amount)                 order_coupon_reduce_amount,
                    sum(original_amount)                      order_original_amount,
                    sum(final_amount)                         order_final_amount
             from dwd_order_info
             group by date_format(create_time, 'yyyy-MM-dd'), user_id
         ),
     tmp_pay as
         (
             select date_format(callback_time, 'yyyy-MM-dd') dt,
                    user_id,
                    count(*)                                 payment_count,
                    sum(payment_amount)                      payment_amount
             from dwd_payment_info
             group by date_format(callback_time, 'yyyy-MM-dd'), user_id
         ),
     tmp_ri as
         (
             select date_format(create_time, 'yyyy-MM-dd') dt,
                    user_id,
                    count(*)                               refund_order_count,
                    sum(refund_num)                        refund_order_num,
                    sum(refund_amount)                     refund_order_amount
             from dwd_order_refund_info
             group by date_format(create_time, 'yyyy-MM-dd'), user_id
         ),
     tmp_rp as
         (
             select date_format(callback_time, 'yyyy-MM-dd') dt,
                    rp.user_id,
                    count(*)                                 refund_payment_count,
                    sum(ri.refund_num)                       refund_payment_num,
                    sum(rp.refund_amount)                    refund_payment_amount
             from (
                      select user_id,
                             order_id,
                             sku_id,
                             refund_amount,
                             callback_time
                      from dwd_refund_payment
                  ) rp
                      left join
                  (
                      select user_id,
                             order_id,
                             sku_id,
                             refund_num
                      from dwd_order_refund_info
                  ) ri
                  on rp.order_id = ri.order_id
                      and rp.sku_id = ri.sku_id
             group by date_format(callback_time, 'yyyy-MM-dd'), rp.user_id
         ),
     tmp_coupon as
         (
             select coalesce(coupon_get.dt, coupon_using.dt, coupon_used.dt)                dt,
                    coalesce(coupon_get.user_id, coupon_using.user_id, coupon_used.user_id) user_id,
                    nvl(coupon_get_count, 0)                                                coupon_get_count,
                    nvl(coupon_using_count, 0)                                              coupon_using_count,
                    nvl(coupon_used_count, 0)                                               coupon_used_count
             from (
                      select date_format(get_time, 'yyyy-MM-dd') dt,
                             user_id,
                             count(*)                            coupon_get_count
                      from dwd_coupon_use
                      where get_time is not null
                      group by user_id, date_format(get_time, 'yyyy-MM-dd')
                  ) coupon_get
                      full outer join
                  (
                      select date_format(using_time, 'yyyy-MM-dd') dt,
                             user_id,
                             count(*)                              coupon_using_count
                      from dwd_coupon_use
                      where using_time is not null
                      group by user_id, date_format(using_time, 'yyyy-MM-dd')
                  ) coupon_using
                  on coupon_get.dt = coupon_using.dt
                      and coupon_get.user_id = coupon_using.user_id
                      full outer join
                  (
                      select date_format(used_time, 'yyyy-MM-dd') dt,
                             user_id,
                             count(*)                             coupon_used_count
                      from dwd_coupon_use
                      where used_time is not null
                      group by user_id, date_format(used_time, 'yyyy-MM-dd')
                  ) coupon_used
                  on nvl(coupon_get.dt, coupon_using.dt) = coupon_used.dt
                      and nvl(coupon_get.user_id, coupon_using.user_id) = coupon_used.user_id
         ),
     tmp_comment as
         (
             select date_format(create_time, 'yyyy-MM-dd') dt,
                    user_id,
                    sum(if(appraise = '1201', 1, 0))       appraise_good_count,
                    sum(if(appraise = '1202', 1, 0))       appraise_mid_count,
                    sum(if(appraise = '1203', 1, 0))       appraise_bad_count,
                    sum(if(appraise = '1204', 1, 0))       appraise_default_count
             from dwd_comment_info
             group by date_format(create_time, 'yyyy-MM-dd'), user_id
         ),
     tmp_od as
         (
             select dt,
                    user_id,
                    collect_set(named_struct('sku_id', sku_id, 'sku_num', sku_num, 'order_count', order_count,
                                             'activity_reduce_amount', activity_reduce_amount, 'coupon_reduce_amount',
                                             coupon_reduce_amount, 'original_amount', original_amount, 'final_amount',
                                             final_amount)) order_detail_stats
             from (
                      select date_format(create_time, 'yyyy-MM-dd')             dt,
                             user_id,
                             sku_id,
                             sum(sku_num)                                       sku_num,
                             count(*)                                           order_count,
                             cast(sum(split_activity_amount) as decimal(16, 2)) activity_reduce_amount,
                             cast(sum(split_coupon_amount) as decimal(16, 2))   coupon_reduce_amount,
                             cast(sum(original_amount) as decimal(16, 2))       original_amount,
                             cast(sum(split_final_amount) as decimal(16, 2))    final_amount
                      from dwd_order_detail
                      group by date_format(create_time, 'yyyy-MM-dd'), user_id, sku_id
                  ) t1
             group by dt, user_id
         )
insert
overwrite
table
dws_user_action_daycount
partition
(
dt
)
select coalesce(tmp_login.user_id, tmp_cf.user_id, tmp_order.user_id, tmp_pay.user_id, tmp_ri.user_id, tmp_rp.user_id,
                tmp_comment.user_id, tmp_coupon.user_id, tmp_od.user_id),
       nvl(login_count, 0),
       nvl(cart_count, 0),
       nvl(favor_count, 0),
       nvl(order_count, 0),
       nvl(order_activity_count, 0),
       nvl(order_activity_reduce_amount, 0),
       nvl(order_coupon_count, 0),
       nvl(order_coupon_reduce_amount, 0),
       nvl(order_original_amount, 0),
       nvl(order_final_amount, 0),
       nvl(payment_count, 0),
       nvl(payment_amount, 0),
       nvl(refund_order_count, 0),
       nvl(refund_order_num, 0),
       nvl(refund_order_amount, 0),
       nvl(refund_payment_count, 0),
       nvl(refund_payment_num, 0),
       nvl(refund_payment_amount, 0),
       nvl(coupon_get_count, 0),
       nvl(coupon_using_count, 0),
       nvl(coupon_used_count, 0),
       nvl(appraise_good_count, 0),
       nvl(appraise_mid_count, 0),
       nvl(appraise_bad_count, 0),
       nvl(appraise_default_count, 0),
       order_detail_stats,
       coalesce(tmp_login.dt, tmp_cf.dt, tmp_order.dt, tmp_pay.dt, tmp_ri.dt, tmp_rp.dt, tmp_comment.dt, tmp_coupon.dt,
                tmp_od.dt)
from tmp_login
         full outer join tmp_cf
                         on tmp_login.user_id = tmp_cf.user_id
                             and tmp_login.dt = tmp_cf.dt
         full outer join tmp_order
                         on coalesce(tmp_login.user_id, tmp_cf.user_id) = tmp_order.user_id
                             and coalesce(tmp_login.dt, tmp_cf.dt) = tmp_order.dt
         full outer join tmp_pay
                         on coalesce(tmp_login.user_id, tmp_cf.user_id, tmp_order.user_id) = tmp_pay.user_id
                             and coalesce(tmp_login.dt, tmp_cf.dt, tmp_order.dt) = tmp_pay.dt
         full outer join tmp_ri
                         on coalesce(tmp_login.user_id, tmp_cf.user_id, tmp_order.user_id, tmp_pay.user_id) =
                            tmp_ri.user_id
                             and coalesce(tmp_login.dt, tmp_cf.dt, tmp_order.dt, tmp_pay.dt) = tmp_ri.dt
         full outer join tmp_rp
                         on coalesce(tmp_login.user_id, tmp_cf.user_id, tmp_order.user_id, tmp_pay.user_id,
                                     tmp_ri.user_id) = tmp_rp.user_id
                             and coalesce(tmp_login.dt, tmp_cf.dt, tmp_order.dt, tmp_pay.dt, tmp_ri.dt) = tmp_rp.dt
         full outer join tmp_comment
                         on coalesce(tmp_login.user_id, tmp_cf.user_id, tmp_order.user_id, tmp_pay.user_id,
                                     tmp_ri.user_id, tmp_rp.user_id) = tmp_comment.user_id
                             and coalesce(tmp_login.dt, tmp_cf.dt, tmp_order.dt, tmp_pay.dt, tmp_ri.dt, tmp_rp.dt) =
                                 tmp_comment.dt
         full outer join tmp_coupon
                         on coalesce(tmp_login.user_id, tmp_cf.user_id, tmp_order.user_id, tmp_pay.user_id,
                                     tmp_ri.user_id, tmp_rp.user_id, tmp_comment.user_id) = tmp_coupon.user_id
                             and coalesce(tmp_login.dt, tmp_cf.dt, tmp_order.dt, tmp_pay.dt, tmp_ri.dt, tmp_rp.dt,
                                          tmp_comment.dt) = tmp_coupon.dt
         full outer join tmp_od
                         on coalesce(tmp_login.user_id, tmp_cf.user_id, tmp_order.user_id, tmp_pay.user_id,
                                     tmp_ri.user_id, tmp_rp.user_id, tmp_comment.user_id, tmp_coupon.user_id) =
                            tmp_od.user_id
                             and coalesce(tmp_login.dt, tmp_cf.dt, tmp_order.dt, tmp_pay.dt, tmp_ri.dt, tmp_rp.dt,
                                          tmp_comment.dt, tmp_coupon.dt) = tmp_od.dt;
-----------------------------------------------------------------------------//（2）每日装载
with tmp_login as
         (
             select user_id,
                    count(*) login_count
             from dwd_page_log
             where dt = '2020-06-15'
               and user_id is not null
               and last_page_id is null
             group by user_id
         ),
     tmp_cf as
         (
             select user_id,
                    sum(if(action_id = 'cart_add', 1, 0))  cart_count,
                    sum(if(action_id = 'favor_add', 1, 0)) favor_count
             from dwd_action_log
             where dt = '2020-06-15'
               and user_id is not null
               and action_id in ('cart_add', 'favor_add')
             group by user_id
         ),
     tmp_order as
         (
             select user_id,
                    count(*)                                  order_count,
                    sum(if(activity_reduce_amount > 0, 1, 0)) order_activity_count,
                    sum(if(coupon_reduce_amount > 0, 1, 0))   order_coupon_count,
                    sum(activity_reduce_amount)               order_activity_reduce_amount,
                    sum(coupon_reduce_amount)                 order_coupon_reduce_amount,
                    sum(original_amount)                      order_original_amount,
                    sum(final_amount)                         order_final_amount
             from dwd_order_info
             where (dt = '2020-06-15' or dt = '9999-99-99')
               and date_format(create_time, 'yyyy-MM-dd') = '2020-06-15'
             group by user_id
         ),
     tmp_pay as
         (
             select user_id,
                    count(*)            payment_count,
                    sum(payment_amount) payment_amount
             from dwd_payment_info
             where dt = '2020-06-15'
             group by user_id
         ),
     tmp_ri as
         (
             select user_id,
                    count(*)           refund_order_count,
                    sum(refund_num)    refund_order_num,
                    sum(refund_amount) refund_order_amount
             from dwd_order_refund_info
             where dt = '2020-06-15'
             group by user_id
         ),
     tmp_rp as
         (
             select rp.user_id,
                    count(*)              refund_payment_count,
                    sum(ri.refund_num)    refund_payment_num,
                    sum(rp.refund_amount) refund_payment_amount
             from (
                      select user_id,
                             order_id,
                             sku_id,
                             refund_amount
                      from dwd_refund_payment
                      where dt = '2020-06-15'
                  ) rp
                      left join
                  (
                      select user_id,
                             order_id,
                             sku_id,
                             refund_num
                      from dwd_order_refund_info
                      where dt >= date_add('2020-06-15', -15)
                  ) ri
                  on rp.order_id = ri.order_id
                      and rp.sku_id = rp.sku_id
             group by rp.user_id
         ),
     tmp_coupon as
         (
             select user_id,
                    sum(if(date_format(get_time, 'yyyy-MM-dd') = '2020-06-15', 1, 0))   coupon_get_count,
                    sum(if(date_format(using_time, 'yyyy-MM-dd') = '2020-06-15', 1, 0)) coupon_using_count,
                    sum(if(date_format(used_time, 'yyyy-MM-dd') = '2020-06-15', 1, 0))  coupon_used_count
             from dwd_coupon_use
             where (dt = '2020-06-15' or dt = '9999-99-99')
               and (date_format(get_time, 'yyyy-MM-dd') = '2020-06-15'
                 or date_format(using_time, 'yyyy-MM-dd') = '2020-06-15'
                 or date_format(used_time, 'yyyy-MM-dd') = '2020-06-15')
             group by user_id
         ),
     tmp_comment as
         (
             select user_id,
                    sum(if(appraise = '1201', 1, 0)) appraise_good_count,
                    sum(if(appraise = '1202', 1, 0)) appraise_mid_count,
                    sum(if(appraise = '1203', 1, 0)) appraise_bad_count,
                    sum(if(appraise = '1204', 1, 0)) appraise_default_count
             from dwd_comment_info
             where dt = '2020-06-15'
             group by user_id
         ),
     tmp_od as
         (
             select user_id,
                    collect_set(named_struct('sku_id', sku_id, 'sku_num', sku_num, 'order_count', order_count,
                                             'activity_reduce_amount', activity_reduce_amount, 'coupon_reduce_amount',
                                             coupon_reduce_amount, 'original_amount', original_amount, 'final_amount',
                                             final_amount)) order_detail_stats
             from (
                      select user_id,
                             sku_id,
                             sum(sku_num)                                       sku_num,
                             count(*)                                           order_count,
                             cast(sum(split_activity_amount) as decimal(16, 2)) activity_reduce_amount,
                             cast(sum(split_coupon_amount) as decimal(16, 2))   coupon_reduce_amount,
                             cast(sum(original_amount) as decimal(16, 2))       original_amount,
                             cast(sum(split_final_amount) as decimal(16, 2))    final_amount
                      from dwd_order_detail
                      where dt = '2020-06-15'
                      group by user_id, sku_id
                  ) t1
             group by user_id
         )
insert
overwrite
table
dws_user_action_daycount
partition
(
dt = '2020-06-15'
)
select coalesce(tmp_login.user_id, tmp_cf.user_id, tmp_order.user_id, tmp_pay.user_id, tmp_ri.user_id, tmp_rp.user_id,
                tmp_comment.user_id, tmp_coupon.user_id, tmp_od.user_id),
       nvl(login_count, 0),
       nvl(cart_count, 0),
       nvl(favor_count, 0),
       nvl(order_count, 0),
       nvl(order_activity_count, 0),
       nvl(order_activity_reduce_amount, 0),
       nvl(order_coupon_count, 0),
       nvl(order_coupon_reduce_amount, 0),
       nvl(order_original_amount, 0),
       nvl(order_final_amount, 0),
       nvl(payment_count, 0),
       nvl(payment_amount, 0),
       nvl(refund_order_count, 0),
       nvl(refund_order_num, 0),
       nvl(refund_order_amount, 0),
       nvl(refund_payment_count, 0),
       nvl(refund_payment_num, 0),
       nvl(refund_payment_amount, 0),
       nvl(coupon_get_count, 0),
       nvl(coupon_using_count, 0),
       nvl(coupon_used_count, 0),
       nvl(appraise_good_count, 0),
       nvl(appraise_mid_count, 0),
       nvl(appraise_bad_count, 0),
       nvl(appraise_default_count, 0),
       order_detail_stats
from tmp_login
         full outer join tmp_cf on tmp_login.user_id = tmp_cf.user_id
         full outer join tmp_order on coalesce(tmp_login.user_id, tmp_cf.user_id) = tmp_order.user_id
         full outer join tmp_pay on coalesce(tmp_login.user_id, tmp_cf.user_id, tmp_order.user_id) = tmp_pay.user_id
         full outer join tmp_ri on coalesce(tmp_login.user_id, tmp_cf.user_id, tmp_order.user_id, tmp_pay.user_id) =
                                   tmp_ri.user_id
         full outer join tmp_rp on coalesce(tmp_login.user_id, tmp_cf.user_id, tmp_order.user_id, tmp_pay.user_id,
                                            tmp_ri.user_id) = tmp_rp.user_id
         full outer join tmp_comment on coalesce(tmp_login.user_id, tmp_cf.user_id, tmp_order.user_id, tmp_pay.user_id,
                                                 tmp_ri.user_id, tmp_rp.user_id) = tmp_comment.user_id
         full outer join tmp_coupon on coalesce(tmp_login.user_id, tmp_cf.user_id, tmp_order.user_id, tmp_pay.user_id,
                                                tmp_ri.user_id, tmp_rp.user_id, tmp_comment.user_id) =
                                       tmp_coupon.user_id
         full outer join tmp_od on coalesce(tmp_login.user_id, tmp_cf.user_id, tmp_order.user_id, tmp_pay.user_id,
                                            tmp_ri.user_id, tmp_rp.user_id, tmp_comment.user_id, tmp_coupon.user_id) =
                                   tmp_od.user_id;

DROP TABLE IF EXISTS dws_sku_action_daycount;
CREATE EXTERNAL TABLE dws_sku_action_daycount
(
    `sku_id`                       STRING COMMENT 'sku_id',
    `order_count`                  BIGINT COMMENT '被下单次数',
    `order_num`                    BIGINT COMMENT '被下单件数',
    `order_activity_count`         BIGINT COMMENT '参与活动被下单次数',
    `order_coupon_count`           BIGINT COMMENT '使用优惠券被下单次数',
    `order_activity_reduce_amount` DECIMAL(16, 2) COMMENT '优惠金额(活动)',
    `order_coupon_reduce_amount`   DECIMAL(16, 2) COMMENT '优惠金额(优惠券)',
    `order_original_amount`        DECIMAL(16, 2) COMMENT '被下单原价金额',
    `order_final_amount`           DECIMAL(16, 2) COMMENT '被下单最终金额',
    `payment_count`                BIGINT COMMENT '被支付次数',
    `payment_num`                  BIGINT COMMENT '被支付件数',
    `payment_amount`               DECIMAL(16, 2) COMMENT '被支付金额',
    `refund_order_count`           BIGINT COMMENT '被退单次数',
    `refund_order_num`             BIGINT COMMENT '被退单件数',
    `refund_order_amount`          DECIMAL(16, 2) COMMENT '被退单金额',
    `refund_payment_count`         BIGINT COMMENT '被退款次数',
    `refund_payment_num`           BIGINT COMMENT '被退款件数',
    `refund_payment_amount`        DECIMAL(16, 2) COMMENT '被退款金额',
    `cart_count`                   BIGINT COMMENT '被加入购物车次数',
    `favor_count`                  BIGINT COMMENT '被收藏次数',
    `appraise_good_count`          BIGINT COMMENT '好评数',
    `appraise_mid_count`           BIGINT COMMENT '中评数',
    `appraise_bad_count`           BIGINT COMMENT '差评数',
    `appraise_default_count`       BIGINT COMMENT '默认评价数'
) COMMENT '每日商品行为'
    PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall/dws/dws_sku_action_daycount/'
    TBLPROPERTIES ("parquet.compression" = "lzo");

with
    tmp_order as (
    select
          sku_id,
          count(*) order_count,
          sum(sku_num) order_count,
          sum(if(split_activity_amount>0,1,0)),
          sum(if(split_coupon_amount>0,1,0)),
          sum(split_activity_amount),
          sum(split_coupon_amount),
          sum(original_amount),
          sum(split_final_amount)
    from dwd_order_detail
    group by sku_id,date_format(create_time,'yyyy-MM-dd')
),
    tmp_payment as(
    select
        sku_id,
        dt,
        count(*),
         sum(sku_num),
        sum(split_final_amount)
    from dwd_order_detail
    group by sku_id,dt
    )

    select
        sku_id,
        date_format(create_time,'yyyy-MM-dd'),
        count(*),
        sum(refund_num),
        sum(refund_amount),

    from dwd_order_refund_info
    group by sku_id,date_format(create_time,'yyyy-MM-dd')

    select

    from dwd_refund_payment

    group by sku_id,create_time;

-------------------------商品主题首日-------------------------------------
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


-------------------------访客主题-----------------------------------
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

----------------------------------活动主题--------------------------------
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

-----------------------------------活动主题首日装载------------------------
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


--------------------------地区主题首日装载------------------------
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
