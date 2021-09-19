create table student(s_id string,s_name string,s_birth string,s_sex string)
row format delimited fields terminated by '\t';

create table course(c_id string,c_name string,t_id string)
row format delimited fields terminated by '\t';

create table teacher(t_id string,t_name string)
row format delimited fields terminated by '\t';

create table score(s_id string,c_id string,s_score int)
row format delimited fields terminated by '\t';

load data local inpath '/export/data/hivedatas/student.csv' overwrite into table student;

load data local inpath '/export/data/hivedatas/course.csv' into table course;

load data local inpath '/export/data/hivedatas/teacher.csv' into table teacher;

load data local inpath '/export/data/hivedatas/score.csv' into table score;

--------------------------------------------------A1
select
*
from (
         select t1.s_id, t1.c_id, t1.s_score
             from (select s_id,
                          c_id,
                          s_score
                   from score
                   where c_id = 01) t1
                      join
                  (select s_id,
                          c_id,
                          s_score
                   from score
                   where c_id = 02) t2
                  on t1.s_id = t2.s_id
             where t1.s_score > t2.s_score
     )t3
join
(
    select
    *
    from
    student
) t4
        on t3.s_id =t4.s_id;
--标答1
select student.*,a.s_score as 01_score,b.s_score as 02_score
from student
  join score a on student.s_id=a.s_id and a.c_id='01'
  left join score b on student.s_id=b.s_id and b.c_id='02'
where  a.s_score>b.s_score;

--标答2
select student.*,a.s_score as 01_score,b.s_score as 02_score
from student
join score a on  a.c_id='01'
join score b on  b.c_id='02'
where  a.s_id=student.s_id and b.s_id=student.s_id and a.s_score>b.s_score;

select
    *
from student

join score s on s.s_id = student.s_id  and s.c_id = '01'
left join score ss on ss.s_id = student.s_id and ss.c_id = '02'
where  s.s_score>ss.s_score;

--------------------------------------------------A2
select
    student.*,
    a.s_score as a_score,
    b.s_score as b_score
from student

join score a on student.s_id = a.s_id and a.c_id = '01'
left join score b on student.s_id = b.s_id and b.c_id = '02'
where a.s_score<b.s_score;

select
    student.*,
    a.s_score,
    b.s_score
from student

join score a on a.c_id = '01'
join score b on b.c_id = '02'
where student.s_id = a.s_id and student.s_id = b.s_id and a.s_score<b.s_score;

--------------------------------------------------A3


