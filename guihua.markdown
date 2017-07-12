##
<input class='input-number' type='number' min='1' max='10' placeholder='1-10'>

![](https://ooo.0o0.ooo/2017/07/06/595e279e09ef3.png)
![](https://ooo.0o0.ooo/2017/07/06/595e2cf015af2.png)
![](https://ooo.0o0.ooo/2017/07/06/595e381630b5f.png)
![](https://ooo.0o0.ooo/2017/07/06/595e3ef12975f.png)
![](https://ooo.0o0.ooo/2017/07/06/595e3f22d0fee.png)
![](https://ooo.0o0.ooo/2017/07/06/595e3fdc93e7b.png)

![](https://ooo.0o0.ooo/2017/07/07/595f3a3d5ac5f.png)





git status
git add spark-normalization_n.sh
git commit -m 'add test.txt'


case when level_standar * pow(0.05, level_num) > 0 and level_num> 1 then '0'
when level_standar * pow(0.05, level_num) > 0 and level_num= 1 then level_standar * pow(0.05, level_num)
ELSE 'level_standar'
case
INSERT INTO TABLE user_youhui_dislike_level PARTITION (ds='20170709')
SELECT device_id,user_id,level_type,level_id,level_standar,CASE WHEN level_standar*pow(0.05,level_num)> 0 AND level_num> 1 THEN '0' WHEN level_standar*pow(0.05,level_num)> 0 AND level_num=1 THEN level_standar*pow($ num,level_num) ELSE 'level_standar' END AS level_standar_new,'0' AS data_type FROM (
SELECT b.device_id,a.user_id,a.level_type,a.level_id,b.level_num,a.level_standar FROM tmp_user_youhui_level a LEFT JOIN tmp_youhui_dislike_level b ON a.user_id=b.user_id AND a.level_id=b.level_id GROUP BY b.device_id,a.user_id,a.level_id,b.level_num,a.level_standar,a.level_type) t;



create table tmp_youhui_level as select device_id,user_id,level_type,level_id,level_standar from level_standar_redis where ds='$day_time';
315	create table tmp_youhui_level as select device_id,user_id,level_type,level_id,level_standar from level_standar_redis where ds='$day_time';
330	select 'youhui_dislike_level';
316	select 'youhui_dislike_level';
331	insert into table youhui_dislike_level partition(ds='$day_time') SELECT device_id, user_id, level_type, level_id, level_standar
317	insert into table youhui_dislike_level partition(ds='$day_time') SELECT device_id, user_id, level_type, level_id, level_standar
332		, case when level_standar * pow($num, level_num) > 0 then level_standar * pow($num, level_num) else level_standar end AS level_standar_new, '0' AS data_type
318		, CASE WHEN level_num> 1 THEN '0' ELSE level_standar * pow($num,level_num)  end AS level_standar_new, '0' AS data_type
333	FROM (SELECT a.device_id, a.user_id, a.level_type, a.level_id, b.level_num
319	FROM (SELECT a.device_id, a.user_id, a.level_type, a.level_id, b.level_num
334			, a.level_standar
320			, a.level_standar
335		FROM tmp_youhui_level a
321		FROM tmp_youhui_level a
...	@@ -338,7 +324,6 @@ FROM (SELECT a.device_id, a.user_id, a.level_type, a.level_id, b.level_num	...	@@ -338,7 +324,6 @@ FROM (SELECT a.device_id, a.user_id, a.level_type, a.level_id, b.level_num
338				AND a.level_id = b.level_id
324				AND a.level_id = b.level_id
339		GROUP BY a.device_id, a.user_id, a.level_id, b.level_num, a.level_standar, a.level_type
325		GROUP BY a.device_id, a.user_id, a.level_id, b.level_num, a.level_standar, a.level_type
340		) t;
