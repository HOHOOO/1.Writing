
hive -e "

set mapred.job.queue.name=q_gmv;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nostrict;
set hive.groupby.skewindata=true;
set hive.exec.parallel=true;
set mapred.reduce.tasks=10;

drop table recommend.tag_relation_cate_user_preference_level_3;
drop table recommend.tag_relation_cate_user_preference_level_3_simple;
drop table recommend.tag_relation_cate_count_left;
drop table recommend.tag_relation_cate_count_right;
drop table recommend.tag_relation_cate_count_cross;
drop table recommend.tag_relation_cate_user_preference_level_3_simple_mirror;
drop table recommend.tag_relation_cate_collaborative;



select '三级品类协同过滤数据源收集';
select '关联三级标签,若不做限制，三级品类105569616，用户数320w';
select '限制时间后，71235142/2458673';
create table recommend.tag_relation_cate_user_preference_level_3 as select * from (SELECT tag_id,user_proxy_key,user_tag_weight,count(*) over (partition by user_proxy_key) tag_sum, row_number () over (partition by user_proxy_key ORDER BY user_tag_weight DESC) rank FROM (select user_proxy_key,tag_id,user_tag_weight from recommend.dw_cp_user_preference_long_term  a left join recommend.sync_level_3_1 b on a.tag_id=b.level_3_id and a.ds>'2017-09-23' where b.level_3_id is not null ) t1 ) t;
select '限制用户最大浏览20 和最小数值0.005 后，12447811/2451193（少的用户是因为权重都小于0.005？）';
CREATE TABLE recommend.tag_relation_cate_user_preference_level_3_simple AS select tag_id,user_proxy_key from create table recommend.tag_relation_cate_user_preference_level_3 where tag_sum>5 and rank<20 and user_tag_weight>0.005;

select '计算标签出现次数';

CREATE TABLE recommend.tag_relation_cate_count_left AS SELECT tag_id,count(DISTINCT user_proxy_key) as user_num,row_number () over (ORDER BY count(DISTINCT user_proxy_key) DESC) rank FROM recommend.tag_relation_cate_user_preference_level_3_simple GROUP BY tag_id having user_num>10;
create table recommend.tag_relation_cate_count_right as select * from recommend.tag_relation_cate_count_left;

select '三级品类协同过滤数据源计算';
select 'cate_cate collaborative';
select '计算共现矩阵';
CREATE TABLE recommend.tag_relation_cate_user_preference_level_3_simple_mirror as select tag_id,user_proxy_key from recommend.tag_relation_cate_user_preference_level_3_simple;
CREATE TABLE recommend.tag_relation_cate_count_cross AS SELECT t.tag_id_1,t.tag_id_2,t.num FROM (SELECT t1.tag_id AS tag_id_1,t2.tag_id AS tag_id_2,count(DISTINCT t2.user_proxy_key) AS num FROM recommend.tag_relation_cate_user_preference_level_3_simple t1 CROSS JOIN recommend.tag_relation_cate_user_preference_level_3_simple_mirror t2 ON t1.user_proxy_key=t2.user_proxy_key WHERE t1.tag_id<> t2.tag_id GROUP BY t1.tag_id,t2.tag_id) t;

select '标签之间相似度';
CREATE TABLE recommend.tag_relation_cate_collaborative AS SELECT t1.tag_id_1 AS tag_id_1,t2.user_num_1 AS user_num_1,t1.tag_id_2 AS tag_id_2,t3.user_num_2 AS user_num_2,t1.num AS num,(t1.num/sqrt(t2.user_num_1*t3.user_num_2)) AS power,row_number () over (ORDER BY (t1.num/sqrt(t2.user_num_1*t3.user_num_2)) DESC) rank FROM recommend.tag_relation_cate_count_cross t1 LEFT JOIN (SELECT tag_id,user_num AS user_num_1 FROM recommend.tag_relation_cate_count_left) t2 ON t1.tag_id_1=t2.tag_id LEFT JOIN (SELECT tag_id,user_num AS user_num_2 FROM recommend.tag_relation_cate_count_right) t3 ON t1.tag_id_2=t3.tag_id GROUP BY t1.tag_id_1,t2.user_num_1,t1.tag_id_2,t3.user_num_2,t1.num,(t1.num/sqrt(t2.user_num_1*t3.user_num_2));



"
