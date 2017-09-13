insert into table user_youhui_dislike_level partition(ds='$day_time') SELECT device_id, user_id, level_type, level_id, level_standar
	, case when level_standar * pow($num, level_num) > 0 and level_num > 1 then 0
		when level_standar * pow($num, level_num) > 0 and level_num = 1 then level_standar * pow($num, level_num)
		else level_standar end AS level_standar_new, '0' AS data_type
FROM (SELECT b.device_id, a.user_id, a.level_type, a.level_id, b.level_num, a.level_standar FROM tmp_user_youhui_level a
		LEFT JOIN tmp_youhui_dislike_level b ON a.user_id = b.user_id
			AND a.level_id = b.level_id
	GROUP BY b.device_id, a.user_id, a.level_id, b.level_num, a.level_standar, a.level_type
	) t;
