


drop table recommend.tag_relation_brand_user_preference;
drop table recommend.tag_relation_brand_user_preference_simple;
drop table recommend.tag_relation_brand_count_left;
drop table recommend.tag_relation_brand_count_right;
drop table recommend.tag_relation_brand_count_cross;
drop table recommend.tag_relation_brand_user_preference_simple_mirror;
drop table recommend.tag_relation_brand_collaborative;
drop table recommend.tag_relation_brand_collaborative_top20;
drop table recommend.tag_relation_brand_collaborative_top20_name;

select '品牌协同过滤数据源收集';
select '若不做限制，三级品类105569616，用户数320w';
select '限制时间后，71235142/2458673';
create table recommend.tag_relation_brand_user_preference as select user_proxy_key, tag_id , user_tag_weight FROM recommend.dw_cp_user_preference_long_term where tag_id like 'brand%' and tag_id<>'brand_0' and ds>'2017-09-23';

select '限制用户最大浏览20 和最小数值0.005 后，439（少的用户是因为权重都小于0.005？）';
CREATE TABLE recommend.tag_relation_brand_user_preference_simple AS select tag_id,user_proxy_key from (SELECT tag_id,user_proxy_key,user_tag_weight,count(*) over (partition by user_proxy_key) tag_sum, row_number () over (partition by user_proxy_key ORDER BY user_tag_weight DESC) rank FROM recommend.tag_relation_brand_user_preference) t where tag_sum>5 and rank<20 and user_tag_weight>0.01;

select '计算标签出现次数';
CREATE TABLE recommend.tag_relation_brand_count_left AS SELECT tag_id,count(DISTINCT user_proxy_key) as user_num,row_number () over (ORDER BY count(DISTINCT user_proxy_key) DESC) rank FROM recommend.tag_relation_brand_user_preference_simple GROUP BY tag_id  having user_num>10;
create table recommend.tag_relation_brand_count_right as select * from recommend.tag_relation_brand_count_left;

tag_relation_cate_user_preference_level_3
create table recommend.tag_relation_brand_user_preference_ori as select a.user_proxy_key,a.tag_id,a.user_tag_weight,b.tag_id_2,b.power,b.rank,a.user_tag_weight*b.power as user_tag_weigh_new from recommend.tag_relation_brand_user_preference a left join (SELECT tag_id_1,tag_id_2,power,rank FROM (SELECT tag_id_1,tag_id_2,power,row_number () over (PARTITION BY tag_id_1 ORDER BY power DESC) rank FROM recommend.tag_relation_brand_collaborative) t1) b on a.tag_id=b.tag_id_1 and b.rank<40 and b.power>0.05;


select '品牌协同过滤数据源计算';
select '计算共现矩阵';
CREATE TABLE recommend.tag_relation_brand_user_preference_simple_mirror as select tag_id,user_proxy_key from recommend.tag_relation_brand_user_preference_simple;
CREATE TABLE recommend.tag_relation_brand_count_cross AS SELECT t.tag_id_1,t.tag_id_2,t.num FROM (SELECT t1.tag_id AS tag_id_1,t2.tag_id AS tag_id_2,count(DISTINCT t2.user_proxy_key) AS num FROM recommend.tag_relation_brand_user_preference_simple t1 CROSS JOIN recommend.tag_relation_brand_user_preference_simple_mirror t2 ON t1.user_proxy_key=t2.user_proxy_key WHERE t1.tag_id<> t2.tag_id GROUP BY t1.tag_id,t2.tag_id) t;

select '标签之间相似度';
CREATE TABLE recommend.tag_relation_brand_collaborative AS SELECT t1.tag_id_1 AS tag_id_1,t2.user_num_1 AS user_num_1,t1.tag_id_2 AS tag_id_2,t3.user_num_2 AS user_num_2,t1.num AS num,(t1.num/sqrt(t2.user_num_1*t3.user_num_2)) AS power,row_number () over (ORDER BY (t1.num/sqrt(t2.user_num_1*t3.user_num_2)) DESC) rank FROM recommend.tag_relation_brand_count_cross t1 LEFT JOIN (SELECT tag_id,user_num AS user_num_1 FROM recommend.tag_relation_brand_count_left) t2 ON t1.tag_id_1=t2.tag_id LEFT JOIN (SELECT tag_id,user_num AS user_num_2 FROM recommend.tag_relation_brand_count_right) t3 ON t1.tag_id_2=t3.tag_id GROUP BY t1.tag_id_1,t2.user_num_1,t1.tag_id_2,t3.user_num_2,t1.num,(t1.num/sqrt(t2.user_num_1*t3.user_num_2));




select "二类偏好聚合"
CREATE TABLE recommend.test_T1_brand as select user_proxy_key,max(user_tag_weigh_new) over (PARTITION BY user_proxy_key,tag_id_2 ) as user_tag_weigh_new,tag_id_2 from  recommend.tag_relation_brand_user_preference_ori;
select "二类偏好与一类去重列转行"
CREATE TABLE recommend.test_T2_brand  as select b.id,b.tag_id_2,b.user_tag_weigh_new from (select user_proxy_key as id ,tag_id_2,user_tag_weigh_new FROM recommend.test_T1_brand) b LEFT OUTER join recommend.tag_relation_brand_user_preference c on b.id=c.user_proxy_key and b.tag_id_2=c.tag_id where c.tag_id is NULL;

CREATE TABLE recommend.tag_relation_brand_user_preference_2 as select * from ( select id as user_proxy_key,tag_id_2 as tag_id,user_tag_weigh_new as user_tag_weight,row_number () over (PARTITION BY id ORDER BY user_tag_weigh_new DESC) as rank from recommend.test_T2_brand) t where t.rank<50;
