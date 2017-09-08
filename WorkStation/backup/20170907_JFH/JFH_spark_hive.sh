#!/bin/bash
source ~/.bash_profile

table_partition=`date -d "$1 day ago" +"%Y%m%d"`



hadoop dfs -rmr /dataOffline/JFH_article_level_num/ds=$table_partition


spark-sql --driver-class-path /data/tmp/zhanshulin/jar/mysql-connector-java-commercial-5.1.40-bin.jar --name "user-portrait-spark-hive" --driver-memory 8g --executor-memory 8G --executor-cores 4 --num-executors 3 --conf "spark.default.parallelism=100" --master yarn --queue q_gmv --master yarn -e"
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nostrict;
set mapred.job.queue.name=q_gmv;

use default;
set hive.map.aggr=true;
set hive.groupby.skewindata=true;
set hive.exec.parallel=true;
set mapred.reduce.tasks=5;
select '删除临时表';
drop table yuanchuang_title_level;
drop table yuanchuang_1;
drop table yuanchuang_2;
drop table yuanchuang_3;
drop table device_articles_request_distinct;
drop table iPhone_device_article;
drop table android_device_article;
drop table JFH_article_level_num_week;
drop table JFH_article_level_num_month;
drop table JFH_article_level_num_month;
drop table JFH_user_article_unique;


select 'iPhone的设备id是用device_id,Android的设备id是使用imei';
select '临时表 device_articles_request_distinct';
create table device_articles_request_distinct as select article_id ,(case when user_id = '' then -1 else user_id end) as user_id ,device_id ,article_type ,imei ,client_type from device_articles_request where ds='$table_partition'  and article_id <> 0 ;

insert into table JFH_device_articles_request_hebing partition(ds='$table_partition') select article_id,user_id,device_id,article_type,client_type,"1"  from device_articles_request_distinct where client_type = 'iPhone' and article_type = 'yh';
insert into table JFH_device_articles_request_hebing partition(ds='$table_partition') select article_id ,user_id ,imei ,article_type,client_type,"1" from device_articles_request_distinct where client_type = 'android' and article_type = 'yh';
select '临时表 iPhone_device_article';
create table iPhone_device_article as select article_id,user_id,device_id,article_type from device_articles_request_distinct where client_type = 'iPhone';
select '临时表 android_device_article';
create table android_device_article as select article_id ,user_id ,imei ,article_type from device_articles_request_distinct where client_type = 'android';
select '合并表 user_article_unique';

select '优惠 iPhone';
create table JFH_user_article_unique as select d1.device_id ,d1.user_id ,d1.article_id ,d1.article_type ,d.pubdatetime ,d.title,'1' as article_channel_id ,'yh' as article_channel_name ,(case when d.level_1 = 'NULL' THEN -1 ELSE d.level_1 END) ,(case when d.level_1_id = 'NULL' THEN -1 ELSE d.level_1_id END) ,(case when d.level_2 = 'NULL' THEN -1 ELSE d.level_2 END) ,(case when d.level_2_id = 'NULL' THEN -1 ELSE d.level_2_id END) ,(case when d.level_3 = 'NULL' THEN -1 ELSE d.level_3 END) ,(case when d.level_3_id = 'NULL' THEN -1 ELSE d.level_3_id END) ,(case when d.level_4 = 'NULL' THEN -1 ELSE d.level_4 END) ,(case when d.level_4_id = 'NULL' THEN -1 ELSE d.level_4_id END) ,d.sync_home ,d.mall from articles_level_lable d left join iPhone_device_article d1 on d.article_id=d1.article_id where d1.article_type='yh';

select '优惠 Android';
insert into table JFH_user_article_unique select d1.imei ,d1.user_id ,d1.article_id ,d1.article_type ,d.pubdatetime ,d.title ,'1' as article_channel_id ,'yh' as article_channel_name ,(case when d.level_1 = 'NULL' THEN -1 ELSE d.level_1 END) ,(case when d.level_1_id = 'NULL' THEN -1 ELSE d.level_1_id END) ,(case when d.level_2 = 'NULL' THEN -1 ELSE d.level_2 END) ,(case when d.level_2_id = 'NULL' THEN -1 ELSE d.level_2_id END) ,(case when d.level_3 = 'NULL' THEN -1 ELSE d.level_3 END) ,(case when d.level_3_id = 'NULL' THEN -1 ELSE d.level_3_id END) ,(case when d.level_4 = 'NULL' THEN -1 ELSE d.level_4 END) ,(case when d.level_4_id = 'NULL' THEN -1 ELSE d.level_4_id END) ,d.sync_home,d.mall from articles_level_lable d left join android_device_article d1 on d.article_id=d1.article_id where d1.article_type='yh';
select '统计天原创和优惠分类的访问次数';

select '优惠一级分类访问次数';
insert into table JFH_article_level_num partition(ds='$table_partition') select t.device_id ,t.user_id ,t.article_type ,"1" as level_type ,t.level_id ,t.level_num ,"1" as article_channel_id from (select device_id ,user_id ,article_type ,level_1_id as level_id ,count(1) as level_num from JFH_user_article_unique and article_type='yh' and level_1_id <> '-1' group by device_id ,user_id ,level_1_id ,article_type) t ;
select '优惠二级分类访问次数';
insert into table JFH_article_level_num partition(ds='$table_partition') select t.device_id ,t.user_id ,t.article_type ,"2" as level_type ,t.level_id ,t.level_num ,"1" as article_channel_id from (select device_id ,user_id ,article_type ,level_2_id as level_id ,count(1) as level_num from JFH_user_article_unique and article_type='yh' and level_2_id <> '-1' group by device_id ,user_id ,level_2_id ,article_type) t ;

drop table JFH_user_article_unique;
drop table device_articles_request_distinct;
drop table iPhone_device_article;
drop table android_device_article;
drop table tmp_article_level_num_month;
drop table article_level_num_month;
select 'end';
exit;
"
