
create table recommend.tag_relation_cate_user_preference as select user_proxy_key,tag_id,user_tag_weight from recommend.dw_cp_user_preference_long_term ;


  CREATE TABLE recommend.tag_relation_collaborative as select * from
  (select * from recommend.tag_relation_cate_collaborative
  UNION ALL
  select * from recommend.tag_relation_brand_collaborative
  );
  CREATE TABLE recommend.tag_relation_collaborative AS SELECT * FROM (SELECT * FROM recommend.tag_relation_cate_collaborative UNION ALL SELECT * FROM recommend.tag_relation_brand_collaborative);
  CREATE TABLE recommend.tag_relation_collaborative AS SELECT*FROM (SELECT*FROM recommend.tag_relation_cate_collaborative UNION ALL SELECT*FROM recommend.tag_relation_brand_collaborative);

CREATE TABLE recommend.tag_relation_collaborative_top20 AS SELECT tag_id_1,tag_id_2,power,rank FROM (SELECT tag_id_1,tag_id_2,power,row_number () over (PARTITION BY tag_id_1 ORDER BY power DESC) rank FROM recommend.tag_relation_collaborative) t1 WHERE t1.rank<40 and t1.power>0.005;

select "连接二类偏好"
create table recommend.tag_relation_cate_user_preference as select a.user_proxy_key,a.tag_id,a.user_tag_weight,b.tag_id_2,b.power,b.rank from recommend.tag_relation_cate_user_preference_level_3 a left join recommend.tag_relation_cate_collaborative b on a.tag_id=b.tag_id_1 and b.rank<40 and b.power>0.005;


recommend.tag_relation_cate_user_preference_level_3

recommend.tag_relation_brand_user_preference





select "二类偏好聚合"
max
select * from recommend.tag_relation_cate_user_preference




select "二类偏好与一类去重列转行"





CREATE TABLE recommend.tag_relation_brand_collaborative_top20 AS SELECT tag_id_1,tag_id_2,power,rank FROM (SELECT tag_id_1,tag_id_2,power,row_number () over (PARTITION BY tag_id_1 ORDER BY power DESC) rank FROM recommend.tag_relation_brand_collaborative) t1 WHERE t1.rank<20;

create table recommend.tag_relation_brand_collaborative_top20_name as select d.*,c.tag_name from recommend.dw_cp_dic_tag_info c right join  (select a.*,b.tag_name as tag_relation_brand from recommend.tag_relation_brand_collaborative_top20 a left join recommend.dw_cp_dic_tag_info b on a.tag_id_1=b.tag_id and b.ds='2017-10-23') d on c.tag_id=d.tag_id_2 and c.ds='2017-10-23';


  CREATE TABLE recommend.tag_relation_cate_collaborative_top20 AS SELECT tag_id_1,tag_id_2,power,rank FROM (SELECT tag_id_1,tag_id_2,power,row_number () over (PARTITION BY tag_id_1 ORDER BY power DESC) rank FROM recommend.tag_relation_cate_collaborative) t1 WHERE t1.rank<20;

  create table recommend.tag_relation_cate_collaborative_top20_name as select d.*,c.tag_name from recommend.dw_cp_dic_tag_info c right join  (select a.*,b.tag_name as tag_relation_cate from recommend.tag_relation_cate_collaborative_top20 a left join recommend.dw_cp_dic_tag_info b on a.tag_id_1=b.tag_id and b.ds='2017-10-23') d on c.tag_id=d.tag_id_2 and c.ds='2017-10-23';
