
select "连接二类偏好";
create table recommend.tag_relation_cate_user_preference as select a.user_proxy_key,a.tag_id,a.user_tag_weight,b.tag_id_2,b.power,b.rank,a.user_tag_weight*b.power as user_tag_weigh_new from recommend.tag_relation_cate_user_preference_level_3 a left join (SELECT tag_id_1,tag_id_2,power,rank FROM (SELECT tag_id_1,tag_id_2,power,row_number () over (PARTITION BY tag_id_1 ORDER BY power DESC) rank FROM recommend.tag_relation_cate_collaborative) t1) b on a.tag_id=b.tag_id_1 and a.rank<30 and b.rank<15 and b.power>0.05;




select "二类偏好聚合";
CREATE TABLE recommend.tag_relation_cate_user_preference_3 as select user_proxy_key as id,tag_id_2,user_tag_weigh_new,row_number() over (PARTITION BY user_proxy_key,tag_id_2 order by user_tag_weigh_new desc ) as row_num from  recommend.tag_relation_cate_user_preference;
select "二类偏好与一类去重列转行";
CREATE TABLE recommend.tag_relation_cate_user_preference_1 as select b.id,b.tag_id_2,b.user_tag_weigh_new,c.tag_id from ( select * from recommend.tag_relation_cate_user_preference_3 where row_num=1 ) b LEFT OUTER join recommend.tag_relation_cate_user_preference_level_3 c on b.id=c.user_proxy_key and b.tag_id_2=c.tag_id and c.rank<20 where c.tag_id is NULL;



CREATE TABLE recommend.tag_relation_cate_user_preference_2 as select * from (select  id as user_proxy_key,tag_id_2 as tag_id,user_tag_weigh_new as user_tag_weight,row_number () over (PARTITION BY id ORDER BY user_tag_weigh_new DESC) as rank from recommend.tag_relation_cate_user_preference_1) t where t.rank<50;


CREATE TABLE recommend.tag_relation_user_preference as Select b.user_proxy_key,b.tag_id,b.user_tag_weight from (
      select a.*,row_number() over (partition by user_proxy_key,tag_id order by user_tag_weight asc) as order_rank
      from (select user_proxy_key,tag_id,cast(user_tag_weight as string) from recommend.tag_relation_cate_user_preference_2
            union all
           select user_proxy_key,tag_id,user_tag_weight from T7 ) a
) b  where order_rank=1;


python $home_path/write_redis_${version}.py $home_path/file/dw_cp_user_preference_${version}_redis_$day_time.txt








输出一个人的一类偏好和二类偏好


recommend.tag_relation_user_preference
recommend.tag_relation_user_preference

recommend.tag_relation_user_preference


recommend.tag_relation_user_preference
recommend.tag_relation_user_preference

redis-cli -h smzdm_bi_rec_cache_redis_m01 keys *shunt*
pull_down_last_time_b:nkk4CzScYwfYMRk2qv6hCgDOFE2CRzCOwAfulukRgoRnxkwz7Adw3g==
history:b:nkk4CzScYwfYMRk2qv6hCgDOFE2CRzCOwAfulukRgoRnxkwz7Adw3g==
history_a_f:b:nkk4CzScYwfYMRk2qv6hCgDOFE2CRzCOwAfulukRgoRnxkwz7Adw3g==

redis-cli -h smzdm_bi_rec_cache_redis_m01 del pull_down_last_time_b:v3AFZ5vVgAOZnoa4khXkqvmcJYJhfCknc0o+tA1yP9zpnZ9UqWFLxw== history:b:v3AFZ5vVgAOZnoa4khXkqvmcJYJhfCknc0o+tA1yP9zpnZ9UqWFLxw== history_a_f:b:v3AFZ5vVgAOZnoa4khXkqvmcJYJhfCknc0o+tA1yP9zpnZ9UqWFLxw==
redis-cli -h smzdm_bi_rec_cache_redis_m01 del pull_down_last_time_b:hy4K6471h/LVi3ng1VjYa6RWzp511qG9iz1AyKCT8JzFnWwHJ7hiwg== history:b:hy4K6471h/LVi3ng1VjYa6RWzp511qG9iz1AyKCT8JzFnWwHJ7hiwg== history_a_f:b:hy4K6471h/LVi3ng1VjYa6RWzp511qG9iz1AyKCT8JzFnWwHJ7hiwg==
redis-cli -h smzdm_bi_rec_cache_redis_m01 del pull_down_last_time_b:W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA== history:b:W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA== history_a_f:b:W1dV74htP2JVokWQQRFVqQXJLj+SZrxNUd6KrFdZhSEF0O/4eVmdxA==
redis-cli -h smzdm_bi_rec_cache_redis_m01 del pull_down_last_time_b:Lurpde80Kxr53+Icp46gwE7kGGv8R21fen1/titV+WtEe3dk+ymyKQ== history:b:Lurpde80Kxr53+Icp46gwE7kGGv8R21fen1/titV+WtEe3dk+ymyKQ== history_a_f:b:Lurpde80Kxr53+Icp46gwE7kGGv8R21fen1/titV+WtEe3dk+ymyKQ==
redis-cli -h smzdm_bi_rec_cache_redis_m01 del pull_down_last_time_b:a7TYdqmg3Y/9NqWXIxmn+YUGV5/OnAsMYpXwKY/ji4f/Mj3WvK5rcQ== history:b:a7TYdqmg3Y/9NqWXIxmn+YUGV5/OnAsMYpXwKY/ji4f/Mj3WvK5rcQ== history_a_f:b:a7TYdqmg3Y/9NqWXIxmn+YUGV5/OnAsMYpXwKY/ji4f/Mj3WvK5rcQ==
redis-cli -h smzdm_bi_rec_cache_redis_m01 del pull_down_last_time_b:44842f22cbf0795749401e373b19397a history:b:44842f22cbf0795749401e373b19397a history_a_f:b:44842f22cbf0795749401e373b19397a
redis-cli -h smzdm_bi_rec_cache_redis_m01 del pull_down_last_time_b:QcxMlY9xbqYTUAfoiiPRI6CrTK8dZcb/m4Diw8TQDmFb29yDSzCWiw== history:b:QcxMlY9xbqYTUAfoiiPRI6CrTK8dZcb/m4Diw8TQDmFb29yDSzCWiw== history_a_f:b:QcxMlY9xbqYTUAfoiiPRI6CrTK8dZcb/m4Diw8TQDmFb29yDSzCWiw==

pull_down_last_time_b:
history:b:
history_a_f:b:
1) "pull_down_last_time_b:a7TYdqmg3Y/9NqWXIxmn+YUGV5/OnAsMYpXwKY/ji4f/Mj3WvK5rcQ==" 2) "dislike:6391431727:a7TYdqmg3Y/9NqWXIxmn+YUGV5/OnAsMYpXwKY/ji4f/Mj3WvK5rcQ==" 3) "proxy:a7TYdqmg3Y/9NqWXIxmn+YUGV5/OnAsMYpXwKY/ji4f/Mj3WvK5rcQ=="
4) "history:b:a7TYdqmg3Y/9NqWXIxmn+YUGV5/OnAsMYpXwKY/ji4f/Mj3WvK5rcQ=="
5) "history_a_f:b:a7TYdqmg3Y/9NqWXIxmn+YUGV5/OnAsMYpXwKY/ji4f/Mj3WvK5rcQ=="



redis-cli -h smzdm_bi_rec_cache_redis_m01 del pull_down_last_time_b:44842f22cbf0795749401e373b19397a history_a_f:b:44842f22cbf0795749401e373b19397a history:b:44842f22cbf0795749401e373b19397a


CREATE TABLE recommend.tag_relation_cate_user_preference_2 as select user_proxy_key,tag_id_2,row_number () over (PARTITION BY user_proxy_key ORDER BY user_tag_weigh_new DESC) as rank FROM ((select user_proxy_key,max(user_tag_weight_new) over (PARTITION BY user_proxy_key,tag_id_2 ) as user_tag_weight_new,tag_id_2 from  recommend.tag_relation_cate_user_preference) b LEFT OUTER recommend.tag_relation_cate_user_preference_level_3 c on b.user_proxy_key=c.user_proxy_key and b.tag_id_2=c.tag_id where c.tag_id is NULL) d where rank<50;




CREATE TABLE recommend.tag_relation_brand_collaborative_top20 AS SELECT tag_id_1,tag_id_2,power,rank FROM (SELECT tag_id_1,tag_id_2,power,row_number () over (PARTITION BY tag_id_1 ORDER BY power DESC) rank FROM recommend.tag_relation_brand_collaborative) t1 WHERE t1.rank<20;

create table recommend.tag_relation_brand_collaborative_name as select d.*,c.tag_name from recommend.dw_cp_dic_tag_info c right join  (select a.*,b.tag_name as tag_relation_brand from (SELECT tag_id_1,tag_id_2,power,rank FROM (SELECT tag_id_1,tag_id_2,power,row_number () over (PARTITION BY tag_id_1 ORDER BY power DESC) rank FROM recommend.tag_relation_brand_collaborative) t1) a left join recommend.dw_cp_dic_tag_info b on a.tag_id_1=b.tag_id and b.ds='2017-10-28' and a.rank<40 and a.power>0.05) d on c.tag_id=d.tag_id_2 and c.ds='2017-10-28' ;
(SELECT tag_id_1,tag_id_2,power,rank FROM (SELECT tag_id_1,tag_id_2,power,row_number () over (PARTITION BY tag_id_1 ORDER BY power DESC) rank FROM recommend.tag_relation_brand_collaborative) t1) b on a.tag_id=b.tag_id_1 and b.rank<40 and b.power>0.05

create table recommend.tag_relation_cate_collaborative_name as select d.*,c.tag_name from recommend.dw_cp_dic_tag_info c right join  (select a.*,b.tag_name as tag_relation_cate from (SELECT tag_id_1,tag_id_2,power,rank FROM (SELECT tag_id_1,tag_id_2,power,row_number () over (PARTITION BY tag_id_1 ORDER BY power DESC) rank FROM recommend.tag_relation_cate_collaborative) t1) a left join recommend.dw_cp_dic_tag_info b on a.tag_id_1=b.tag_id and b.ds='2017-10-28' and a.rank<40 and a.power>0.05) d on c.tag_id=d.tag_id_2 and c.ds='2017-10-28' ;



  CREATE TABLE recommend.tag_relation_cate_collaborative_top20 AS SELECT tag_id_1,tag_id_2,power,rank FROM (SELECT tag_id_1,tag_id_2,power,row_number () over (PARTITION BY tag_id_1 ORDER BY power DESC) rank FROM recommend.tag_relation_cate_collaborative) t1 WHERE t1.rank<20;

  create table recommend.tag_relation_cate_collaborative_top20_name as select d.*,c.tag_name from recommend.dw_cp_dic_tag_info c right join  (select a.*,b.tag_name as tag_relation_cate from recommend.tag_relation_cate_collaborative_top20 a left join recommend.dw_cp_dic_tag_info b on a.tag_id_1=b.tag_id and b.ds='2017-10-23') d on c.tag_id=d.tag_id_2 and c.ds='2017-10-23';
