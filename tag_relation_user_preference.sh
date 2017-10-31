
select "连接二类偏好";
create table recommend.tag_relation_cate_user_preference as select a.user_proxy_key,a.tag_id,a.user_tag_weight,b.tag_id_2,b.power,b.rank,a.user_tag_weight*b.power as user_tag_weigh_new from recommend.tag_relation_cate_user_preference_level_3 a left join (SELECT tag_id_1,tag_id_2,power,rank FROM (SELECT tag_id_1,tag_id_2,power,row_number () over (PARTITION BY tag_id_1 ORDER BY power DESC) rank FROM recommend.tag_relation_cate_collaborative) t1) b on a.tag_id=b.tag_id_1 and a.rank<30 and b.rank<15 and b.power>0.05;

select "二类偏好聚合";
CREATE TABLE recommend.tag_relation_cate_user_preference_3 as select user_proxy_key as id,tag_id_2,user_tag_weigh_new,row_number() over (PARTITION BY user_proxy_key,tag_id_2 order by user_tag_weigh_new desc ) as row_num from  recommend.tag_relation_cate_user_preference;
select "二类偏好与一类去重";
CREATE TABLE recommend.tag_relation_cate_user_preference_1 as select b.id,b.tag_id_2,b.user_tag_weigh_new,c.tag_id from ( select * from recommend.tag_relation_cate_user_preference_3 where row_num=1 ) b LEFT OUTER join recommend.tag_relation_cate_user_preference_level_3 c on b.id=c.user_proxy_key and b.tag_id_2=c.tag_id and c.rank<20 where c.tag_id is NULL;

CREATE TABLE recommend.tag_relation_cate_user_preference_2 as select * from (select  id as user_proxy_key,tag_id_2 as tag_id,user_tag_weigh_new as user_tag_weight,row_number () over (PARTITION BY id ORDER BY user_tag_weigh_new DESC) as rank from recommend.tag_relation_cate_user_preference_1) t where t.rank<50;

CREATE TABLE recommend.tag_relation_user_preference as Select b.user_proxy_key,b.tag_id,b.user_tag_weight from (
      select a.*,row_number() over (partition by user_proxy_key,tag_id order by user_tag_weight asc) as order_rank
      from (select user_proxy_key,tag_id,cast(user_tag_weight as string) from recommend.tag_relation_cate_user_preference_2
            union all
           select user_proxy_key,tag_id,user_tag_weight from T7 ) a
) b  where order_rank=1;
