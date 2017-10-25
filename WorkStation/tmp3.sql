
DROP TABLE recommend.quality_data_source_newton;
DROP TABLE recommend.quality_data_source_wilson;
DROP TABLE recommend.quality_data_source;
DROP TABLE recommend.quality_data_score;

CREATE TABLE recommend.quality_data_source_newton AS SELECT id, (1*collection_count/max_collection_count)+(2*love_rating_count/max_love_rating_count)+(3*comment_count/max_comment_count)+(4*reward_count/max_reward_count) AS score,0 as last_status, 0 as increase_rate, 0 as order_rank, score_timestamp FROM (SELECT id,collection_count,love_rating_count,comment_count,reward_count,score_timestamp,max(collection_count) over (PARTITION BY score_timestamp ) AS max_collection_count,max(love_rating_count) over (PARTITION BY score_timestamp ) AS max_love_rating_count,max(comment_count) over (PARTITION BY score_timestamp ) AS max_comment_count,max(reward_count) over (PARTITION BY score_timestamp ) AS max_reward_count from  ( SELECT id,collection_count,love_rating_count,comment_count,reward_count,"$score_timestamp" as score_timestamp FROM sync_yuanchuang ) t2 ) t3;

CREATE TABLE recommend.quality_data_source_wilson AS SELECT id, CASE WHEN phat=-1.96 THEN 0 WHEN phat<>-1.96 AND source_from=5 THEN 0.8*(phat + 1.96/n - 1.96*sqrt((phat * (1- phat) /n)+1.96/(4*pow(n,2))))/(1+1.96/n) ELSE (phat + 1.96/n - 1.96*sqrt((phat * (1- phat) /n)+1.96/(4*pow(n,2))))/(1+1.96/n)  END as score,0 as last_status, 0 as increase_rate, 0 as order_rank, score_timestamp from ( SELECT id,(worthy+unworthy) as n,CASE WHEN (worthy+unworthy)=0 THEN -1.96 ELSE (worthy/(worthy+unworthy)) END as phat,worthy,unworthy,source_from,"$score_timestamp" as score_timestamp FROM sync_youhui ) t ;

CREATE TABLE recommend.quality_data_source AS SELECT a.* from (select * from recommend.quality_data_source_newton union all  select * from recommend.quality_data_source_wilson) a;

INSERT OVERWRITE TABLE recommend.quality_data_score
Select b.*,(score - last_status)/last_status as increase_rate  from (
      select a.*,LEAD(score, 1, 0) over (partition by id order by score_timestamp desc) as last_status ,rank() over (partition by id order by score_timestamp desc) as order_rank
      from (select id, score,score_timestamp from recommend.quality_data_score WHERE order_rank=1
            union all
           select id, score,score_timestamp  from recommend.quality_data_source ) a
) b  where order_rank<2 ;











SELECT * ,  OVER (PARTITION BY deviceid ORDER BY actual_start) AS start2
SELECT AVG(wilson_lower),source_from FROM wilson_data_lower GROUP BY source_from LIMIT 100;
SELECT "原创热度计算 热度值 牛顿冷却值"

select sum(collection_count),sum(love_rating_count),sum(comment_count),sum(reward_count) from sync_yuanchuang  limit 5;

select max(collection_count),max(love_rating_count),, from sync_yuanchuang  limit 5;

 limit 5


INSERT OVERWRITE  TABLE quality_newton_score
SELECT * FROM  quality_newton_score UNION ALL quality_newton_data_source



 as collection_count,
as love_rating_count,
 as comment_count,
 as reward_count,


/max()
/max()
/max(reward_count)


limit 5;
,max(love_rating_count),max(comment_count),max(reward_count)

SELECT AVG(wilson_lower),source_from FROM wilson_data_lower GROUP BY source_from LIMIT 100;

select AVG(wilson_lower),count(1),std(wilson_lower) ,source_from from wilson_data_lower group by source_from;
worthy              	int
unworthy            	int
source_from 1、5被降权
