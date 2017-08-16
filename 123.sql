
不加insert表示发生降权的数据条数
加insert表示数据为计算偏好为0的数据（理论上为本月统计周期内未浏览的插入数据）

portrait_summary.name   portrait_summary.summary        portrait_summary.ds
youhui_dislike_tag      232   20170814
youhui_dislike_tag_insert       60551 20170814[理论上应为437条，多余数据均为错误的device_id]
本问题影响也不算大，仅当这部分设备id有问题的用户没有user_id时有影响，8894/5425343（8.14当天全量浏览数据）
user_youhui_dislike_level       164   20170814
user_youhui_dislike_level_insert        1588  20170814
user_youhui_dislike_brand       84    20170814
user_youhui_dislike_brand_insert        478   20170814
user_youhui_dislike_tag 198   20170814
user_youhui_dislike_tag_insert  1097  2017081
youhui_dislike_level    192   20170814
youhui_dislike_level_insert     1565  20170814[理论上为1564条，多余数据均为错误的device_id]
youhui_dislike_brand    100   20170814
youhui_dislike_brand_insert     462   20170814
yuanchuang_dislike_level        4     20170814
yuanchuang_dislike_level_insert 1657  20170814
user_yuanchuang_dislike_level   4     20170814
user_yuanchuang_dislike_level_insert    1654  20170814
user_yuanchuang_dislike_brand   2     20170814
user_yuanchuang_dislike_brand_insert    10    20170814
user_yuanchuang_dislike_tag     2     20170814
user_yuanchuang_dislike_tag_insert      246   20170814
yuanchuang_dislike_tag  2     20170814
yuanchuang_dislike_tag_insert   51    20170814
yuanchuang_dislike_brand        2     20170814
yuanchuang_dislike_brand_insert 10    20170814

两个问题：
第一，device_id有异常时   是否应让降权生效
第二，标签计算结果为0 的问题（数量较少）、品类计算结果为0的问题（数量较多）

fake_level: 1756
fake_tag: 669
fake_brand: 562
select COUNT(*) from youhui_dislike_tag where ds='20170814' and device_id='5284047f4ffb4e04824a2fd1d1f0cd62' and user_id='-1';
1461
select COUNT(*) from youhui_dislike_tag where ds='20170814' and tag_standar=0 and device_id='5284047f4ffb4e04824a2fd1d1f0cd62' and user_id='-1';
484


select count(*) from fake_youhui_dislike_level where device_id='5284047f4ffb4e04824a2fd1d1f0cd62'
0
select count(*) from fake_youhui_dislike_brand where device_id='5284047f4ffb4e04824a2fd1d1f0cd62'
0

hive -e "create table if not exists user_visited_article(id string,
device_id string,
user_id string,
article_id string,
article_channel_id string,
id_1 string,
create_time string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'"

CREATE TABLE `level_id_type`(
  `level_id` int,
  `level_type` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://hadoopcluster/user/hive/warehouse/level_id_type'

  CREATE EXTERNAL TABLE `portrait_summary`(
    `index` int,
    `name` string,
    `summary` double
    )
  PARTITIONED BY (
    `ds` string)
  ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\u0001'
  STORED AS INPUTFORMAT
    'org.apache.hadoop.mapred.TextInputFormat'
  OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
  LOCATION
    'hdfs://hadoopcluster/dataOffline/portrait_summary'

#降权数据统计
insert into table portrait_summary partition(ds='20170814') select 'youhui_dislike_level' as name, count(*) as summary from youhui_dislike_level where ds='20170814' and level_standar<>level_standar_new ;
insert into table portrait_summary partition(ds='20170814') select 'youhui_dislike_level_insert' as name, count(*) as summary from youhui_dislike_level where ds='20170814' and level_standar=0;

insert into table portrait_summary partition(ds='20170814') select 'youhui_dislike_brand' as name, count(*) as summary from youhui_dislike_brand where ds='20170814' and brand_level_standar<>brand_level_standar_new ;
insert into table portrait_summary partition(ds='20170814') select 'youhui_dislike_brand_insert' as name, count(*) as summary from youhui_dislike_brand where ds='20170814' and brand_level_standar=0;

insert into table portrait_summary partition(ds='20170814') select 'youhui_dislike_tag' as name, count(*) as summary from youhui_dislike_tag where ds='20170814' and tag_standar<>tag_standar_new ;
insert into table portrait_summary partition(ds='20170814') select 'youhui_dislike_tag_insert' as name, count(*) as summary from youhui_dislike_tag where ds='20170814' and tag_standar=0;


insert into table portrait_summary partition(ds='20170814') select 'user_youhui_dislike_level' as name, count(*) as summary from user_youhui_dislike_level where ds='20170814' and level_standar<>level_standar_new ;
insert into table portrait_summary partition(ds='20170814') select 'user_youhui_dislike_level_insert' as name, count(*) as summary from user_youhui_dislike_level where ds='20170814' and level_standar=0;

insert into table portrait_summary partition(ds='20170814') select 'user_youhui_dislike_brand' as name, count(*) as summary from user_youhui_dislike_brand where ds='20170814' and brand_level_standar<>brand_level_standar_new ;
insert into table portrait_summary partition(ds='20170814') select 'user_youhui_dislike_brand_insert' as name, count(*) as summary from user_youhui_dislike_brand where ds='20170814' and brand_level_standar=0;

insert into table portrait_summary partition(ds='20170814') select 'user_youhui_dislike_tag' as name, count(*) as summary from user_youhui_dislike_tag where ds='20170814' and tag_standar<>tag_standar_new ;
insert into table portrait_summary partition(ds='20170814') select 'user_youhui_dislike_tag_insert' as name, count(*) as summary from user_youhui_dislike_tag where ds='20170814' and tag_standar=0;



insert into table portrait_summary partition(ds='20170814') select 'yuanchuang_dislike_level' as name, count(*) as summary from yuanchuang_dislike_level where ds='20170814' and level_standar<>level_standar_new ;
insert into table portrait_summary partition(ds='20170814') select 'yuanchuang_dislike_level_insert' as name, count(*) as summary from yuanchuang_dislike_level where ds='20170814' and level_standar=0;

insert into table portrait_summary partition(ds='20170814') select 'yuanchuang_dislike_brand' as name, count(*) as summary from yuanchuang_dislike_brand where ds='20170814' and brand_level_standar<>brand_level_standar_new ;
insert into table portrait_summary partition(ds='20170814') select 'yuanchuang_dislike_brand_insert' as name, count(*) as summary from yuanchuang_dislike_brand where ds='20170814' and brand_level_standar=0;

insert into table portrait_summary partition(ds='20170814') select 'yuanchuang_dislike_tag' as name, count(*) as summary from yuanchuang_dislike_tag where ds='20170814' and tag_standar<>tag_standar_new ;
insert into table portrait_summary partition(ds='20170814') select 'yuanchuang_dislike_tag_insert' as name, count(*) as summary from yuanchuang_dislike_tag where ds='20170814' and tag_standar=0;


insert into table portrait_summary partition(ds='20170814') select 'user_yuanchuang_dislike_level' as name, count(*) as summary from user_yuanchuang_dislike_level where ds='20170814' and level_standar<>level_standar_new ;
insert into table portrait_summary partition(ds='20170814') select 'user_yuanchuang_dislike_level_insert' as name, count(*) as summary from user_yuanchuang_dislike_level where ds='20170814' and level_standar=0;

insert into table portrait_summary partition(ds='20170814') select 'user_yuanchuang_dislike_brand' as name, count(*) as summary from user_yuanchuang_dislike_brand where ds='20170814' and brand_level_standar<>brand_level_standar_new ;
insert into table portrait_summary partition(ds='20170814') select 'user_yuanchuang_dislike_brand_insert' as name, count(*) as summary from user_yuanchuang_dislike_brand where ds='20170814' and brand_level_standar=0;

insert into table portrait_summary partition(ds='20170814') select 'user_yuanchuang_dislike_tag' as name, count(*) as summary from user_yuanchuang_dislike_tag where ds='20170814' and tag_standar<>tag_standar_new ;
insert into table portrait_summary partition(ds='20170814') select 'user_yuanchuang_dislike_tag_insert' as name, count(*) as summary from user_yuanchuang_dislike_tag where ds='20170814' and tag_standar=0;





#数据结果质量




insert into table portrait_summary partition(ds='20170814') select 'youhui_level_1' as name, count(*) as summary from youhui_level_1 where level_str is NULL ;
insert into table portrait_summary partition(ds='20170814') select 'youhui_level_3_4' as name, count(*) as summary from youhui_level_3_4 where level is NULL ;
insert into table portrait_summary partition(ds='20170814') select 'tmp_youhui_brand' as name, count(*) as summary from tmp_youhui_brand where brand_str is NULL ;
insert into table portrait_summary partition(ds='20170814') select 'tmp_youhui_tag_2' as name, count(*) as summary from tmp_youhui_tag_2 where tag_str is NULL ;
insert into table portrait_summary partition(ds='20170814') select 'user_youhui_level_1' as name, count(*) as summary from user_youhui_level_1 where level_str is NULL ;
insert into table portrait_summary partition(ds='20170814') select 'user_youhui_level_3_4' as name, count(*) as summary from user_youhui_level_3_4 where level is NULL ;
insert into table portrait_summary partition(ds='20170814') select 'tmp_user_youhui_brand' as name, count(*) as summary from tmp_user_youhui_brand where brand_str is NULL ;
insert into table portrait_summary partition(ds='20170814') select 'tmp_user_youhui_tag_2' as name, count(*) as summary from tmp_user_youhui_tag_2 where tag_str is NULL ;
insert into table portrait_summary partition(ds='20170814') select 'yuanchuang_level_3_4' as name, count(*) as summary from yuanchuang_level_3_4 where level_str is NULL ;
insert into table portrait_summary partition(ds='20170814') select 'tmp_yuanchuang_brand' as name, count(*) as summary from tmp_yuanchuang_brand where brand_str is NULL ;
insert into table portrait_summary partition(ds='20170814') select 'tmp_yuanchuang_tag_1' as name, count(*) as summary from tmp_yuanchuang_tag_1 where tag_str is NULL ;
insert into table portrait_summary partition(ds='20170814') select 'user_yuanchuang_level_3_4' as name, count(*) as summary from user_yuanchuang_level_3_4 where level_str is NULL ;
insert into table portrait_summary partition(ds='20170814') select 'tmp_user_yuanchuang_brand' as name, count(*) as summary from tmp_user_yuanchuang_brand where brand_str is NULL ;
insert into table portrait_summary partition(ds='20170814') select 'tmp_user_yuanchuang_tag_1' as name, count(*) as summary from tmp_user_yuanchuang_tag_1 where tag_str is NULL ;



insert into table portrait_summary partition(ds='20170814') select 'yuanchuang_dislike_level_insert' as name, count(*) as summary from user_yuanchuang_dislike_level where ds='20170814' and level_standar=0;

insert into table portrait_summary partition(ds='20170814') select 'yuanchuang_dislike_level' as name, count(*) as summary from user_yuanchuang_dislike_level where ds='20170814' and level_standar<>level_standar_new ;
insert into table portrait_summary partition(ds='20170814') select 'yuanchuang_dislike_level_insert' as name, count(*) as summary from user_yuanchuang_dislike_level where ds='20170814' and level_standar=0;

insert into table portrait_summary partition(ds='20170814') select 'yuanchuang_dislike_level' as name, count(*) as summary from user_yuanchuang_dislike_level where ds='20170814' and level_standar<>level_standar_new ;
insert into table portrait_summary partition(ds='20170814') select 'yuanchuang_dislike_level_insert' as name, count(*) as summary from user_yuanchuang_dislike_level where ds='20170814' and level_standar=0;








youhui_level_1 level_str
youhui_level_3_4 level
tmp_youhui_brand brand_str
tmp_youhui_tag_2 tag_str
user_youhui_level_1 level_str
user_youhui_level_3_4 level
tmp_user_youhui_brand brand_str
tmp_user_youhui_tag_2 tag_str
yuanchuang_level_3_4 level_str
tmp_yuanchuang_brand brand_str
tmp_yuanchuang_tag_1 tag_str
user_yuanchuang_level_3_4 level_str
tmp_user_yuanchuang_brand brand_str
tmp_user_yuanchuang_tag_1 tag_str

youhui_level

		空值数
			空值为未求出数值或异常极端数据产生

		空值数

	空值数

	空值数

	空值数

	空值数

	空值数


		空值数

		空值数

	空值数

	空值数

	空值数

	空值数

	空值数



( select count(*) from user_yuanchuang_dislike_level where level_standar<>level_standar_new ) as yuanchuang_dislike_level,
( select count(*) from user_youhui_dislike_level where level_standar<>level_standar_new ) as yuanchuang_dislike_level,
( select count(*) from user_youhui_dislike_level where level_standar<>level_standar_new ) as yuanchuang_dislike_level,
( select count(*) from user_youhui_dislike_level where level_standar<>level_standar_new ) as yuanchuang_dislike_level,
( select count(*) from user_youhui_dislike_level where level_standar<>level_standar_new ) as yuanchuang_dislike_level,
( select count(*) from user_youhui_dislike_level where level_standar<>level_standar_new ) as yuanchuang_dislike_level,

device_id,user_id,brand,level_3_type,level_3_id,level_3,count(1) as brand_level_num from default.article_brand_yh_month_par where ds=$day_time and level_3_type = 3 and level_3_id <> '-1' group by device_id,user_id,brand,level_3_type,level_3_id,level_3;













curl -i -X POST -H "'Content-type':'application/x-www-form-urlencoded', 'charset':'utf-8', 'Accept': 'text/plain'" -d 'data={"user": "zhangguoqiang@smzdm.com", "password": "Newpasswd1_", "sender": "zhangguoqiang@smzdm.com", "smtpServer": "smtp.smzdm.com", "recipient": ["lijing@smzdm.com", "zhangguoqiang@smzdm.com"], "subject": "lijingeeeeerb", "text": "11111 <br> 2222<br>" }' "http://hadoop004:1090/email/alarm"
