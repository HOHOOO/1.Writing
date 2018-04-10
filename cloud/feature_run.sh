CREATE TABLE recommend.FEATURE_USER_AGE_SEX AS SELECT * FROM recommend.zhimafen_update a left join recommend.feature_up_level1 b on a.userid=b.device_id_level1 left join recommend.feature_up_level2 c on a.userid=c.device_id_level2 left join recommend.feature_up_level3 d on a.userid=d.device_id_level3 left join recommend.feature_click_level1 e on a.userid=e.device_id_click_level1 left join recommend.feature_click_level2 f on a.userid=f.device_id_click_level2 left join recommend.feature_click_level3 g on a.userid=g.device_id_click_level3;

CREATE TABLE recommend.FEATURE_USER_AGE_SEX_TEST AS SELECT * FROM recommend.feature_up_level1 a left join recommend.feature_up_level2 c on a.device_id_level1=c.device_id_level2 left join recommend.feature_up_level3 d on a.device_id_level1=d.device_id_level3 left join recommend.feature_click_level1 e on a.device_id_level1=e.device_id_click_level1 left join recommend.feature_click_level2 f on a.device_id_level1=f.device_id_click_level2 left join recommend.feature_click_level3
 g on a.device_id_level1=g.device_id_click_level3;

#create table recommend.zhimafen_update as select userid,province,age,zhimafen,sex,case when  age+1<18 then 1 else 0 end as age_label_0,case when  age+1>=16 and age+1<28 then 1 else 0 end as age_label_1,case when  age+1>=25 and age+1<45 then 1 else 0 end as age_label_2,case when  age+1>=40 and age+1<55 then 1 else 0 end as age_label_3,case when  age+1>=55 then 1 else 0 end as age_label_4 from recommend.zhimafen;

#ALTER TABLE recommend.feature_up_level3 CHANGE user_proxy_key device_id_level3 string;
#ALTER TABLE recommend.feature_click_level1 CHANGE device_id device_id_click_level1 string;
#ALTER TABLE recommend.feature_click_level2 CHANGE device_id device_id_click_level2 string;
#ALTER TABLE recommend.feature_click_level3 CHANGE user_proxy_key device_id_click_level3 string;
#ALTER TABLE recommend.feature_click_level2 CHANGE cate_9 cat_9 string;
