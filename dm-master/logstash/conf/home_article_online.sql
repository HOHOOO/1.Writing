select id,
article_id,
channel_id,
channel,
article_channel,
level1_ids,
level2_ids,
level3_ids,
level4_ids,
tag_ids,
brand_ids,
sync_home,
is_top,
machine_report,
date_format(publish_time,"%Y-%m-%d %H:%i:%S")  as publish_time,
date_format(sync_home_time,"%Y-%m-%d %H:%i:%S")  as sync_home_time,
date_format(sync_time,"%Y-%m-%d %H:%i:%S")  as sync_time,
date_format(auto_updatetime,"%Y-%m-%d %H:%i:%S")  as auto_updatetime,
status
from home_article_online
where auto_updatetime >= :sql_last_value

