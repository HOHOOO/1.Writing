
select "连接二类偏好"
create table recommend.tag_relation_cate_user_preference as select a.user_proxy_key,a.tag_id,a.user_tag_weight,b.tag_id_2,b.power,b.rank,a.user_tag_weight*b.power as user_tag_weigh_new from recommend.tag_relation_cate_user_preference_level_3 a left join (SELECT tag_id_1,tag_id_2,power,rank FROM (SELECT tag_id_1,tag_id_2,power,row_number () over (PARTITION BY tag_id_1 ORDER BY power DESC) rank FROM recommend.tag_relation_cate_collaborative) t1) b on a.tag_id=b.tag_id_1 and b.rank<40 and b.power>0.05;


create table recommend.tag_relation_brand_user_preference_ori as select a.user_proxy_key,a.tag_id,a.user_tag_weight,b.tag_id_2,b.power,b.rank,a.user_tag_weight*b.power as user_tag_weigh_new from recommend.tag_relation_brand_user_preference a left join (SELECT tag_id_1,tag_id_2,power,rank FROM (SELECT tag_id_1,tag_id_2,power,row_number () over (PARTITION BY tag_id_1 ORDER BY power DESC) rank FROM recommend.tag_relation_brand_collaborative) t1) b on a.tag_id=b.tag_id_1 and b.rank<40 and b.power>0.03;


create table recommend.tag_relation_cate_user_preference as select  0 as user_proxy_key,tag_id_1 as tag_id,0 as user_tag_weight,tag_id_2,power,0 as rank,0 as user_tag_weight_new from tag_relation_cate_collaborative limit 500;



select "二类偏好聚合"
CREATE TABLE recommend.test_T1 as select user_proxy_key,max(user_tag_weigh_new) over (PARTITION BY user_proxy_key,tag_id_2 ) as user_tag_weigh_new,tag_id_2 from  recommend.tag_relation_cate_user_preference;
select "二类偏好与一类去重列转行"
CREATE TABLE recommend.test_T2  as select b.id,b.tag_id_2,b.user_tag_weigh_new from (select user_proxy_key as id ,tag_id_2,user_tag_weigh_new FROM recommend.test_T1) b LEFT OUTER join recommend.tag_relation_cate_user_preference_level_3 c on b.id=c.user_proxy_key and b.tag_id_2=c.tag_id where c.tag_id is NULL;

CREATE TABLE recommend.tag_relation_cate_user_preference_2 as select id as user_proxy_key,tag_id_2 as tag_id,user_tag_weigh_new as user_tag_weight,row_number () over (PARTITION BY user_proxy_key ORDER BY user_tag_weigh_new DESC) as rank from recommend.test_T2 where rank<50;



select "二类偏好聚合"
CREATE TABLE recommend.test_T1_brand as select user_proxy_key,max(user_tag_weigh_new) over (PARTITION BY user_proxy_key,tag_id_2 ) as user_tag_weigh_new,tag_id_2 from  recommend.tag_relation_brand_user_preference_ori;
select "二类偏好与一类去重列转行"
CREATE TABLE recommend.test_T2_brand  as select b.id,b.tag_id_2,b.user_tag_weigh_new from (select user_proxy_key as id ,tag_id_2,user_tag_weigh_new FROM recommend.test_T1_brand) b LEFT OUTER join recommend.tag_relation_brand_user_preference c on b.id=c.user_proxy_key and b.tag_id_2=c.tag_id where c.tag_id is NULL;

CREATE TABLE recommend.tag_relation_brand_user_preference_2 as select id as user_proxy_key,tag_id_2 as tag_id,user_tag_weigh_new as user_tag_weight,row_number () over (PARTITION BY user_proxy_key ORDER BY user_tag_weigh_new DESC) as rank from recommend.test_T2_brand where rank<50;

CREATE TABLE recommend.tag_relation_user_preference as (select user_proxy_key,tag_id,user_tag_weight from  recommend.tag_relation_cate_user_preference_2 union all select user_proxy_key,tag_id,user_tag_weight from recommend.tag_relation_brand_user_preference_2)








CREATE TABLE recommend.tag_relation_cate_user_preference_2 as select user_proxy_key,tag_id_2,row_number () over (PARTITION BY user_proxy_key ORDER BY user_tag_weigh_new DESC) as rank FROM ((select user_proxy_key,max(user_tag_weight_new) over (PARTITION BY user_proxy_key,tag_id_2 ) as user_tag_weight_new,tag_id_2 from  recommend.tag_relation_cate_user_preference) b LEFT OUTER recommend.tag_relation_cate_user_preference_level_3 c on b.user_proxy_key=c.user_proxy_key and b.tag_id_2=c.tag_id where c.tag_id is NULL) d where rank<50;




CREATE TABLE recommend.tag_relation_brand_collaborative_top20 AS SELECT tag_id_1,tag_id_2,power,rank FROM (SELECT tag_id_1,tag_id_2,power,row_number () over (PARTITION BY tag_id_1 ORDER BY power DESC) rank FROM recommend.tag_relation_brand_collaborative) t1 WHERE t1.rank<20;

create table recommend.tag_relation_brand_collaborative_top20_name as select d.*,c.tag_name from recommend.dw_cp_dic_tag_info c right join  (select a.*,b.tag_name as tag_relation_brand from recommend.tag_relation_brand_collaborative_top20 a left join recommend.dw_cp_dic_tag_info b on a.tag_id_1=b.tag_id and b.ds='2017-10-23') d on c.tag_id=d.tag_id_2 and c.ds='2017-10-23';


  CREATE TABLE recommend.tag_relation_cate_collaborative_top20 AS SELECT tag_id_1,tag_id_2,power,rank FROM (SELECT tag_id_1,tag_id_2,power,row_number () over (PARTITION BY tag_id_1 ORDER BY power DESC) rank FROM recommend.tag_relation_cate_collaborative) t1 WHERE t1.rank<20;

  create table recommend.tag_relation_cate_collaborative_top20_name as select d.*,c.tag_name from recommend.dw_cp_dic_tag_info c right join  (select a.*,b.tag_name as tag_relation_cate from recommend.tag_relation_cate_collaborative_top20 a left join recommend.dw_cp_dic_tag_info b on a.tag_id_1=b.tag_id and b.ds='2017-10-23') d on c.tag_id=d.tag_id_2 and c.ds='2017-10-23';
