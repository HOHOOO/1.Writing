#!/bin/bash

source ~/.bash_profile


data=$1
month_time=$2
hive_table1=$3
hive_table2=$4
echo $data
echo $hive_table1
echo $hive_table2
hdfs dfs -rm -r -f -skipTrash /dataOffline/$hive_table2/ds=$data
spark-sql --driver-class-path /data/tmp/zhanshulin/jar/mysql-connector-java-commercial-5.1.40-bin.jar --name 分类标准化 --driver-memory 8g --executor-memory 8G --executor-cores 4 --num-executors 3 --conf "spark.default.parallelism=100" --master yarn --queue q_gmv --master yarn -e"
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nostrict;
set mapred.job.queue.name=q_gmv;

use default;
set hive.map.aggr=true;
set hive.groupby.skewindata=true;
set hive.exec.parallel=true;
set mapred.reduce.tasks=10;
select '删除临时表 ';


drop table article_level_num_month;
drop table tmp_leve_1_avg_1;
drop table tmp_leve_2_avg_1;
drop table tmp_leve_1_avg_11;
drop table tmp_leve_2_avg_11;
drop table tmp_leve_1_num_1;
drop table tmp_leve_2_num_1;
drop table tmp_leve_1_num_11;
drop table tmp_leve_2_num_11;
DROP TABLE  tmp_article_level_num_month;
drop TABLE  article_level_num_month;


select '统计最近30天原创和优惠的分类访问次数';
select '临时表 tmp_article_level_num_month';
create table tmp_article_level_num_month as select device_id ,user_id ,article_channel_name ,level_type ,level_id ,level_num ,article_channel_id from JFH_article_level_num where ds between '$month_time' and '$data' ;
select '临时表 article_level_num_month';
create table article_level_num_month as select device_id ,user_id ,article_channel_name ,level_type ,level_id ,sum(level_num) as level_num ,article_channel_id from tmp_article_level_num_month group by device_id ,user_id ,article_channel_name ,level_type ,level_id ,article_channel_id;


select '临时表 tmp_leve_1_avg_1';
create table tmp_leve_1_avg_1 as select device_id,user_id,article_channel_id,stddev(level_num) as stddev,avg(level_num) as avg from article_level_num_month where level_type = 1 and article_channel_id = 1 group by device_id,user_id,article_channel_id;
select '临时表 tmp_leve_2_avg_1';
create table tmp_leve_2_avg_1 as select device_id,user_id,article_channel_id,stddev(level_num) as stddev,avg(level_num) as avg from article_level_num_month where level_type = 2 and article_channel_id = 1 group by device_id,user_id,article_channel_id;


select '临时表 tmp_leve_1_num_1';
create table tmp_leve_1_num_1 as select t1.device_id,t1.user_id,t2.article_channel_name,t2.level_type,t2.level_id,t2.level_num,t1.article_channel_id,t1.stddev,t1.avg from tmp_leve_1_avg_1 t1 join article_level_num_month t2 on t1.device_id=t2.device_id and t1.user_id=t2.user_id and t1.article_channel_id=t2.article_channel_id where t2.level_type = 1 and t1.device_id <> '';
select '临时表 tmp_leve_2_num_1';
create table tmp_leve_2_num_1 as select t1.device_id,t1.user_id,t2.article_channel_name,t2.level_type,t2.level_id,t2.level_num,t1.article_channel_id,t1.stddev,t1.avg from tmp_leve_2_avg_1 t1 join article_level_num_month t2 on t1.device_id=t2.device_id and t1.user_id=t2.user_id and t1.article_channel_id=t2.article_channel_id where t2.level_type = 2 and t1.device_id <> '';


select '插入优惠 1 级分类标准化';
insert into table $hive_table2 partition(ds='$data') select device_id,user_id,article_channel_name,level_type,level_id,level_num,(case level_standar when 'NaN' then 0 else level_standar end) as level_standar,article_channel_id from (select device_id,user_id,article_channel_name,level_type,level_id,level_num,round((level_num-avg) / stddev , 4) as level_standar , article_channel_id from tmp_leve_1_num_1 group by device_id,user_id,article_channel_name,level_type,level_id,level_num,avg,stddev,article_channel_id) t ;
select '插入优惠 2 级分类标准化';
insert into table $hive_table2 partition(ds='$data') select device_id,user_id,article_channel_name,level_type,level_id,level_num,(case level_standar when 'NaN' then 0 else level_standar end) as level_standar,article_channel_id from (select device_id,user_id,article_channel_name,level_type,level_id,level_num,round((level_num-avg) / stddev , 4) as level_standar , article_channel_id from tmp_leve_2_num_1 group by device_id,user_id,article_channel_name,level_type,level_id,level_num,avg,stddev,article_channel_id) t ;





exit;
"
