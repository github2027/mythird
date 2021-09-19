drop table if exists test_one;
create table test_one(
    userId string comment '用户id',
    visitDate string comment '访问日期',
    visitCount bigint comment '访问次数'
) comment '第一题'
row format delimited fields terminated by '\t';

insert into table test_one values('u01','2017/1/21',5);
insert into table test_one values('u02','2017/1/23',6);
insert into table test_one values('u03','2017/1/22',8);
insert into table test_one values('u04','2017/1/20',3);
insert into table test_one values('u01','2017/1/23',6);
insert into table test_one values('u01','2017/2/21',8);
insert into table test_one values('u02','2017/1/23',6);
insert into table test_one values('u01','2017/2/22',4);

drop table if exists test_two;
create table test_two(
    shoop_name string COMMENT '店铺名称',
    user_id string COMMENT '用户id',
    visit_time string COMMENT '访问时间'
)
row format delimited fields terminated by '\t';

insert into table test_two values ('huawei','1001','2017-02-10');
insert into table test_two values ('icbc','1001','2017-02-10');
insert into table test_two values ('huawei','1001','2017-02-10');
insert into table test_two values ('apple','1001','2017-02-10');
insert into table test_two values ('huawei','1001','2017-02-10');
insert into table test_two values ('huawei','1002','2017-02-10');
insert into table test_two values ('huawei','1002','2017-02-10');
insert into table test_two values ('huawei','1001','2017-02-10');
insert into table test_two values ('huawei','1003','2017-02-10');
insert into table test_two values ('huawei','1004','2017-02-10');
insert into table test_two values ('huawei','1005','2017-02-10');
insert into table test_two values ('icbc','1002','2017-02-10');
insert into table test_two values ('jingdong','1006','2017-02-10');
insert into table test_two values ('jingdong','1003','2017-02-10');
insert into table test_two values ('jingdong','1002','2017-02-10');
insert into table test_two values ('jingdong','1004','2017-02-10');
insert into table test_two values ('apple','1001','2017-02-10');
insert into table test_two values ('apple','1001','2017-02-10');
insert into table test_two values ('apple','1001','2017-02-10');
insert into table test_two values ('apple','1002','2017-02-10');
insert into table test_two values ('apple','1002','2017-02-10');
insert into table test_two values ('apple','1005','2017-02-10');
insert into table test_two values ('apple','1005','2017-02-10');
insert into table test_two values ('apple','1006','2017-02-10');

drop table if exists test_three_ORDER;
create table test_three_ORDER
(
    `Date` String COMMENT '下单时间',
    `Order_id` String COMMENT '订单ID',
    `User_id` String COMMENT '用户ID',
    `amount` decimal(10,2) COMMENT '金额'
)
row format delimited fields terminated by '\t';

insert into table test_three_ORDER values ('2017-10-01','10029011','1000003251',19.50);
insert into table test_three_ORDER values ('2017-10-03','10029012','1000003251',29.50);
insert into table test_three_ORDER values ('2017-10-04','10029013','1000003252',39.50);
insert into table test_three_ORDER values ('2017-10-05','10029014','1000003253',49.50);
insert into table test_three_ORDER values ('2017-11-01','10029021','1000003251',130.50);
insert into table test_three_ORDER values ('2017-11-03','10029022','1000003251',230.50);
insert into table test_three_ORDER values ('2017-11-04','10029023','1000003252',330.50);
insert into table test_three_ORDER values ('2017-11-05','10029024','1000003253',430.50);
insert into table test_three_ORDER values ('2017-11-07','10029025','1000003254',530.50);
insert into table test_three_ORDER values ('2017-11-15','10029026','1000003255',630.50);
insert into table test_three_ORDER values ('2017-12-01','10029027','1000003252',112.50);
insert into table test_three_ORDER values ('2017-12-03','10029028','1000003251',212.50);
insert into table test_three_ORDER values ('2017-12-04','10029029','1000003253',312.50);
insert into table test_three_ORDER values ('2017-12-05','10029030','1000003252',412.50);
insert into table test_three_ORDER values ('2017-12-07','10029031','1000003258',512.50);
insert into table test_three_ORDER values ('2017-12-15','10029032','1000003255',612.50);

drop table if exists test_four_log;
create table test_four_user(
    user_id string COMMENT '用户ID',
    name string COMMENT '用户姓名',
    age int COMMENT '用户年龄'
)
row format delimited fields terminated by '\t';

drop table if exists test_four_log;
create table test_four_log(
    user_id string COMMENT '用户ID',
    url string COMMENT '链接'
)
row format delimited fields terminated by '\t';

insert into table test_four_user values ('1','1',8);
insert into table test_four_user values ('2','2',45);
insert into table test_four_user values ('3','3',14);
insert into table test_four_user values ('4','4',18);
insert into table test_four_user values ('5','5',17);
insert into table test_four_user values ('6','6',19);
insert into table test_four_user values ('7','7',26);
insert into table test_four_user values ('8','8',22);
insert into table test_four_log values('1','111');
insert into table test_four_log values('2','111');
insert into table test_four_log values('3','111');
insert into table test_four_log values('4','111');
insert into table test_four_log values('5','111');
insert into table test_four_log values('6','111');
insert into table test_four_log values('7','111');
insert into table test_four_log values('8','111');
insert into table test_four_log values('1','111');
insert into table test_four_log values('2','111');
insert into table test_four_log values('3','111');
insert into table test_four_log values('4','111');
insert into table test_four_log values('5','111');
insert into table test_four_log values('6','111');
insert into table test_four_log values('7','111');
insert into table test_four_log values('8','111');
insert into table test_four_log values('1','111');
insert into table test_four_log values('2','111');
insert into table test_four_log values('3','111');
insert into table test_four_log values('4','111');
insert into table test_four_log values('5','111');
insert into table test_four_log values('6','111');
insert into table test_four_log values('7','111');
insert into table test_four_log values('8','111');

create table test_five_active(
    active_time string COMMENT '活跃日期',
    user_id string COMMENT '用户id',
    age int COMMENT '用户年龄'
)
row format delimited fields terminated by '\t';

insert into table test_five_active values ('11','test_1',11);
insert into table test_five_active values ('11','test_2',22);
insert into table test_five_active values ('11','test_3',33);
insert into table test_five_active values ('11','test_4',44);

insert into table test_five_active values ('12','test_3',33);
insert into table test_five_active values ('12','test_5',55);
insert into table test_five_active values ('12','test_6',66);

insert into table test_five_active values ('13','test_4',44);
insert into table test_five_active values ('13','test_5',55);
insert into table test_five_active values ('13','test_7',77);

create table test_six_ordertable
(
    `userid` string COMMENT '购买用户',
    `money` decimal(10,2) COMMENT '金额',
    `paymenttime` string COMMENT '购买时间',
    `orderid` string COMMENT '订单id'
)
row format delimited fields terminated by '\t';
insert into table test_six_ordertable values('1',1,'2017-09-01','1');
insert into table test_six_ordertable values('2',2,'2017-09-02','2');
insert into table test_six_ordertable values('3',3,'2017-09-03','3');
insert into table test_six_ordertable values('4',4,'2017-09-04','4');

insert into table test_six_ordertable values('3',5,'2017-10-05','5');
insert into table test_six_ordertable values('6',6,'2017-10-06','6');
insert into table test_six_ordertable values('1',7,'2017-10-07','7');
insert into table test_six_ordertable values('8',8,'2017-10-09','8');
insert into table test_six_ordertable values('6',6,'2017-10-16','60');
insert into table test_six_ordertable values('1',7,'2017-10-17','70');

create table test_seven_BOOK
(
    BOOK_ID String COMMENT '总编号',
    SORT String COMMENT '分类号',
    BOOK_NAME String COMMENT '书名',
    WRITER String COMMENT '作者',
    OUTPUT String COMMENT '出版单位',
    PRICE decimal(10,2) COMMENT '单价'
)
row format delimited fields terminated by '\t';

create table test_seven_READER
(
    READER_ID String COMMENT '借书证号',
    COMPANY String COMMENT '单位',
    NAME String COMMENT '姓名',
    SEX String COMMENT '性别',
    GRADE String COMMENT '职称',
    ADDR String COMMENT '地址'
)
row format delimited fields terminated by '\t';

create table test_seven_BORROW_LOG
(
    READER_ID String COMMENT '借书证号',
    BOOK_D String COMMENT '总编号',
    BORROW_ATE date COMMENT '借书日期'
)
row format delimited fields terminated by '\t';

insert into table test_seven_book values ('1001','A1','Java','James Gosling','sun','11');
insert into table test_seven_book values ('1002','A2','linux','Linus Benedict Torvalds','sun','22');
insert into table test_seven_book values ('1003','A3','Java3','James Gosling3','sun3','33');
insert into table test_seven_book values ('1004','A4','Java4','James Gosling4','sun4','44');
insert into table test_seven_book values ('1005','B1','Java5','James Gosling5','sun','55');
insert into table test_seven_book values ('1006','C1','Java6','James Gosling6','sun5','66');
insert into table test_seven_book values ('1007','D1','Java7','James Gosling7','sun6','77');
insert into table test_seven_book values ('1008','E1','Java8','James Gosling4','sun3','88');
insert into table test_seven_reader values ('7','buu',decode(binary('李大帅'),'utf-8'),'man','lay1','beijing4');
insert into table test_seven_reader values ('2','buu2','苏大强','man','lay2','beijing2');
insert into table test_seven_reader values ('3','buu2','李二胖','woman','lay3','beijing3');
insert into table test_seven_reader values ('4','buu3','王三涛','man','lay4','beijing4');
insert into table test_seven_reader values ('5','buu4','刘四虎','woman','lay5','beijing1');
insert into table test_seven_reader values ('6','buu','宋冬野','woman','lay6','beijing5');
insert into table test_seven_borrow_log values ('1','1002','2019-06-01');
insert into table test_seven_borrow_log values ('1','1003','2019-06-02');
insert into table test_seven_borrow_log values ('1','1006','2019-06-03');
insert into table test_seven_borrow_log values ('2','1001','2019-06-04');
insert into table test_seven_borrow_log values ('3','1002','2019-06-05');
insert into table test_seven_borrow_log values ('4','1005','2019-06-06');
insert into table test_seven_borrow_log values ('5','1003','2019-06-06');
insert into table test_seven_borrow_log values ('3','1006','2019-06-07');
insert into table test_seven_borrow_log values ('2','1003','2019-06-03');
insert into table test_seven_borrow_log values ('3','1008','2019-06-03');
insert into table test_seven_borrow_log values ('1','1002','2019-06-04');

create table test_eight_serverlog
(
    server_time string COMMENT '时间',
    server_api  string comment '接口',
    server_ip string COMMENT 'ip地址'
)
row format delimited fields terminated by '\t';
insert into table test_eight_serverlog values ('2016-11-09 11:22:05','/api/user/login','110.23.5.33');
insert into table test_eight_serverlog values ('2016-11-09 11:23:10','/api/user/detail','57.3.2.16');
insert into table test_eight_serverlog values ('2016-11-09 14:59:40','/api/user/login','200.6.5.161');
insert into table test_eight_serverlog values ('2016-11-09 14:22:05','/api/user/login','110.23.5.32');
insert into table test_eight_serverlog values ('2016-11-09 14:23:10','/api/user/detail','57.3.2.13');
insert into table test_eight_serverlog values ('2016-11-09 14:59:40','/api/user/login','200.6.5.164');
insert into table test_eight_serverlog values ('2016-11-09 14:59:40','/api/user/login','200.6.5.165');
insert into table test_eight_serverlog values ('2016-11-09 14:22:05','/api/user/login','110.23.5.36');
insert into table test_eight_serverlog values ('2016-11-09 14:23:10','/api/user/detail','57.3.2.17');
insert into table test_eight_serverlog values ('2016-11-09 14:59:40','/api/user/login','200.6.5.168');
insert into table test_eight_serverlog values ('2016-11-09 14:59:40','/api/user/login','200.6.5.168');
insert into table test_eight_serverlog values ('2016-11-09 14:22:05','/api/user/login','110.23.5.32');
insert into table test_eight_serverlog values ('2016-11-09 14:23:10','/api/user/detail','57.3.2.13');
insert into table test_eight_serverlog values ('2016-11-09 14:59:40','/api/user/login','200.6.5.164');
insert into table test_eight_serverlog values ('2016-11-09 15:22:05','/api/user/login','110.23.5.33');
insert into table test_eight_serverlog values ('2016-11-09 15:23:10','/api/user/detail','57.3.2.16');
insert into table test_eight_serverlog values ('2016-11-09 15:59:40','/api/user/login','200.6.5.166');

create table test_nine_credit_log(
    dist_id string COMMENT '区组id',
    account string COMMENT '账号',
    `money` decimal(10,2) COMMENT '充值金额',
    create_time string COMMENT '订单时间'
)
row format delimited fields terminated by '\t';
insert into table test_nine_credit_log values ('1','11',100006,'2019-01-02 13:00:01');
insert into table test_nine_credit_log values ('1','12',110000,'2019-01-02 13:00:02');
insert into table test_nine_credit_log values ('1','13',102000,'2019-01-02 13:00:03');
insert into table test_nine_credit_log values ('1','14',100300,'2019-01-02 13:00:04');
insert into table test_nine_credit_log values ('1','15',100040,'2019-01-02 13:00:05');
insert into table test_nine_credit_log values ('1','18',110000,'2019-01-02 13:00:02');
insert into table test_nine_credit_log values ('1','16',100005,'2019-01-03 13:00:06');
insert into table test_nine_credit_log values ('1','17',180000,'2019-01-03 13:00:07');


insert into table test_nine_credit_log values ('2','21',100800,'2019-01-02 13:00:11');
insert into table test_nine_credit_log values ('2','22',100030,'2019-01-02 13:00:12');
insert into table test_nine_credit_log values ('2','23',100000,'2019-01-02 13:00:13');
insert into table test_nine_credit_log values ('2','24',100010,'2019-01-03 13:00:14');
insert into table test_nine_credit_log values ('2','25',100070,'2019-01-03 13:00:15');
insert into table test_nine_credit_log values ('2','26',100800,'2019-01-02 15:00:11');

insert into table test_nine_credit_log values ('3','31',106000,'2019-01-02 13:00:08');
insert into table test_nine_credit_log values ('3','32',100400,'2019-01-02 13:00:09');
insert into table test_nine_credit_log values ('3','33',100030,'2019-01-02 13:00:10');
insert into table test_nine_credit_log values ('3','34',100003,'2019-01-02 13:00:20');
insert into table test_nine_credit_log values ('3','35',100020,'2019-01-02 13:00:30');
insert into table test_nine_credit_log values ('3','36',100500,'2019-01-02 13:00:40');
insert into table test_nine_credit_log values ('3','37',106000,'2019-01-03 13:00:50');
insert into table test_nine_credit_log values ('3','38',100800,'2019-01-03 13:00:59');

drop table if exists `test_ten_account`;
create table `test_ten_account`(
    `dist_id` string COMMENT '区组id',
    `account` string COMMENT '账号',
    `gold` bigint COMMENT '金币'
)
row format delimited fields terminated by '\t';

insert into table test_ten_account values ('1','11',100006);
insert into table test_ten_account values ('1','12',110000);
insert into table test_ten_account values ('1','13',102000);
insert into table test_ten_account values ('1','14',100300);
insert into table test_ten_account values ('1','15',100040);
insert into table test_ten_account values ('1','18',110000);
insert into table test_ten_account values ('1','16',100005);
insert into table test_ten_account values ('1','17',180000);

insert into table test_ten_account values ('2','21',100800);
insert into table test_ten_account values ('2','22',100030);
insert into table test_ten_account values ('2','23',100000);
insert into table test_ten_account values ('2','24',100010);
insert into table test_ten_account values ('2','25',100070);
insert into table test_ten_account values ('2','26',100800);

insert into table test_ten_account values ('3','31',106000);
insert into table test_ten_account values ('3','32',100400);
insert into table test_ten_account values ('3','33',100030);
insert into table test_ten_account values ('3','34',100003);
insert into table test_ten_account values ('3','35',100020);
insert into table test_ten_account values ('3','36',100500);
insert into table test_ten_account values ('3','37',106000);
insert into table test_ten_account values ('3','38',100800);

--会员表
drop table if exists test_eleven_member;
create table test_eleven_member(
    memberid string COMMENT '会员id',
    credits bigint COMMENT '积分'
)
row format delimited fields terminated by '\t';
--销售表
drop table if exists test_eleven_sale;
create table test_eleven_sale(
    memberid string COMMENT '会员id',
    MNAccount decimal(10,2) COMMENT '购买金额'
)
row format delimited fields terminated by '\t';
--退货表
drop table if exists test_eleven_regoods;
create table test_eleven_regoods(
    memberid string COMMENT '会员id',
    RMNAccount decimal(10,2) COMMENT '退货金额'
)
row format delimited fields terminated by '\t';

insert into table test_eleven_member values('1001',0);
insert into table test_eleven_member values('1002',0);
insert into table test_eleven_member values('1003',0);
insert into table test_eleven_member values('1004',0);
insert into table test_eleven_member values('1005',0);
insert into table test_eleven_member values('1006',0);
insert into table test_eleven_member values('1007',0);

insert into table test_eleven_sale values('1001',5000);
insert into table test_eleven_sale values('1002',4000);
insert into table test_eleven_sale values('1003',5000);
insert into table test_eleven_sale values('1004',6000);
insert into table test_eleven_sale values('1005',7000);
insert into table test_eleven_sale values('1004',3000);
insert into table test_eleven_sale values('1002',6000);
insert into table test_eleven_sale values('1001',2000);
insert into table test_eleven_sale values('1004',3000);
insert into table test_eleven_sale values('1006',3000);
insert into table test_eleven_sale values(NULL,1000);
insert into table test_eleven_sale values(NULL,1000);
insert into table test_eleven_sale values(NULL,1000);
insert into table test_eleven_sale values(NULL,1000);

insert into table test_eleven_regoods values('1001',1000);
insert into table test_eleven_regoods values('1002',1000);
insert into table test_eleven_regoods values('1003',1000);
insert into table test_eleven_regoods values('1004',1000);
insert into table test_eleven_regoods values('1005',1000);
insert into table test_eleven_regoods values('1002',1000);
insert into table test_eleven_regoods values('1001',1000);
insert into table test_eleven_regoods values('1003',1000);
insert into table test_eleven_regoods values('1002',1000);
insert into table test_eleven_regoods values('1005',1000);
insert into table test_eleven_regoods values(NULL,1000);
insert into table test_eleven_regoods values(NULL,1000);
insert into table test_eleven_regoods values(NULL,1000);
insert into table test_eleven_regoods values(NULL,1000);

create table test_twelve_student
(
    id bigint comment '学号',
    name string comment '姓名',
    age bigint comment '年龄'
)
row format delimited fields terminated by '\t';

create table test_twelve_course
(
    cid string comment '课程号,001/002格式',
    cname string comment '课程名'
)
row format delimited fields terminated by '\t';

Create table test_twelve_score
(
    id bigint comment '学号',
    cid string comment '课程号',
    score bigint comment '成绩'
)
row format delimited fields terminated by '\t';

insert into table test_twelve_student values (1001,'wsl1',21);
insert into table test_twelve_student values (1002,'wsl2',22);
insert into table test_twelve_student values (1003,'wsl3',23);
insert into table test_twelve_student values (1004,'wsl4',24);
insert into table test_twelve_student values (1005,'wsl5',25);

insert into table test_twelve_course values ('001','math');
insert into table test_twelve_course values ('002','English');
insert into table test_twelve_course values ('003','Chinese');
insert into table test_twelve_course values ('004','music');

insert into table test_twelve_score values (1001,'004',10);
insert into table test_twelve_score values (1002,'003',21);
insert into table test_twelve_score values (1003,'002',32);
insert into table test_twelve_score values (1004,'001',43);
insert into table test_twelve_score values (1005,'003',54);
insert into table test_twelve_score values (1001,'002',65);
insert into table test_twelve_score values (1002,'004',76);
insert into table test_twelve_score values (1003,'002',77);
insert into table test_twelve_score values (1001,'004',48);
insert into table test_twelve_score values (1002,'003',39);