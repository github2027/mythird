set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;


select
    mid_id,
    session_id,
    count(*),
    sum(during_time)
from
(
select
    mid_id,
    last_page_id,
    page_id,
    during_time,
    ts,
    last_value(session_start_point,true) over(partition by mid_id order by ts) session_time,
    concat(mid_id,'-',last_value(session_start_point,true) over(partition by mid_id order by ts)) session_id
from
(
select
    mid_id,
    last_page_id,
    page_id,
    during_time,
    ts,
   if(last_page_id is null,ts,null) session_start_point
    from dwd_page_log
    where dt ='2020-06-14'
    )t1
)t2
group by mid_id, session_id;

---------------------------方法二
select
    mid_id,
    session,
    count(*),
    sum(during_time)
from
(
 select
    mid_id,
    last_page_id,
    page_id,
    during_time,
    ts,
   concat(mid_id,'-',sum(session_first_point)  over(partition by mid_id order by ts) ) session
from
(select
    mid_id,
    last_page_id,
    page_id,
    during_time,
    ts,
    if(last_page_id is null,1,0) session_first_point
from dwd_page_log
    where dt='2020-06-14')t1
    )t2
group by mid_id, session;

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


select
    '2020-06-14',
    count(distinct(mid_id)),
    sum(during_time) /1000,
    avg(during_time) /1000,
    sum(page_count),
    avg(page_count),
    count(*),
    sum(if(page_count=1,1,0)),
    sum(if(page_count=1,1,0))/count(*)*100,
    t3.channel
from
(select
    mid_id,
    session,
    count(*) page_count,
    sum(during_time) during_time,
    channel
from
(
 select
    mid_id,
    last_page_id,
    page_id,
    during_time,
    ts,
    channel,
   concat(mid_id,'-',sum(session_first_point)  over(partition by mid_id order by ts) ) session
from
(select
    mid_id,
    last_page_id,
    page_id,
    during_time,
    ts,
    if(last_page_id is null,1,0) session_first_point,
    channel
from dwd_page_log
where dt='2020-06-14')t1
    )t2
group by mid_id, channel,session)t3;

-------------------------增加渠道------------------------------
select
    '2020-06-14',
    is_new,
    channel,
    recent_days,
    count(distinct(mid_id)),
    sum(during_time) /1000,
    avg(during_time) /1000,
    sum(page_count),
    avg(page_count),
    count(*),
    sum(if(page_count=1,1,0)),
    sum(if(page_count=1,1,0))/count(*)*100
from
(select
    mid_id,
    channel,
    is_new,
    recent_days,
    session,
    count(*) page_count,
    sum(during_time) during_time
from
(
 select
    t1.mid_id,
    last_page_id,
    page_id,
    during_time,
    ts,
    channel,
    concat(t1.mid_id,'-',sum(session_first_point)  over(partition by t1.mid_id,recent_days order by ts) ) session,
    if(visit_date_first>=date_add('2020-06-14',-recent_days+1),1,0) is_new,
    recent_days
from
(select
    mid_id,
    last_page_id,
    page_id,
    during_time,
    ts,
    channel,
    if(last_page_id is null,1,0) session_first_point
from dwd_page_log
where dt='2020-06-14')t1
    left join
    (
    select
        mid_id,
        visit_date_first,
        recent_days
    from dwt_visitor_topic
    lateral view explode (`array`(1,7,30)) tmp as recent_days
    where dt >= date_add('2020-06-14',-29)
    and dt >= date_add('2020-06-14',-recent_days+1)
        )t0
    on t1.mid_id =t0.mid_id
    )t2
group by mid_id, is_new,channel, recent_days,session)t3
group by is_new,channel,recent_days;


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

--------------------------------------⭐-------------------------------------------
select
       mid_id,
       session,
       count(*),
       sum(during_time)
from
(select mid_id,
        last_page_id,
        page_id,
        ts,
        during_time,
        concat(mid_id, '->', last_value(start_point, true) over (partition by mid_id order by ts)) session
 from (select mid_id,
              last_page_id,
              page_id,
              ts,
              during_time,
              if(last_page_id is null, ts, null) start_point
       from dwd_page_log
       where dt = '2020-06-14'
      ) t1
)t2
group by mid_id,session;

select
    mid_id,
    session_id,
    count(*) page_count,
    sum(during_time) during_time
from
(
    select
        mid_id,
        last_page_id,
        page_id,
        ts,
        during_time,
        session_start_point,
        concat(mid_id,'-',last_value(session_start_point,true) over(partition by mid_id order by ts)) session_id
    from
    (
        select
            mid_id,
            last_page_id,
            page_id,
            ts,
            during_time,
            if(last_page_id is null,ts,null) session_start_point
        from dwd_page_log
        where dt='2020-06-14'
    ) t1
) t2
group by mid_id,session_id;

select
       '2020-06-14',
      count(distinct mid_id),
       sum(during_time),
       avg(during_time),
       sum(page_count),
       avg(page_count),
       count(*) ,
       sum(if(page_count=1,1,0)) ,
      sum(if(page_count=1,1,0))/count(*) *100
from
(select
        mid_id,
        session,
        sum(during_time) during_time,
        count(*) page_count
 from (select mid_id,
              last_page_id,
              page_id,
              ts,
              during_time,
              concat(mid_id, '-',
                     last_value(if(last_page_id is null, ts, null)) over (partition by mid_id order by ts)) session
       from dwd_page_log
       where dt = '2020-06-14'
      ) t1
    group by mid_id, session
)t2
;





