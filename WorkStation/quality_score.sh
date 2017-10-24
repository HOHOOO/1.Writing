#!/bin/bash
home_path=$(cd "`dirname $0`"; pwd)
quality_sync_time=`date -d "4 day ago" +"%Y-%m-%d"`



hive -e 'CREATE EXTERNAL TABLE IF NOT EXISTS `sync_yuanchuang` (`id` INT,`uid` INT,`anonymous` INT,`type` INT,`title` string,`image` string,`comment_count` INT,`collection_count` INT,`love_rating_count` INT,`brand` string,`link` string,`mall` string,`is_delete` INT,`status` INT,`publishtime` string,`sum_collect_comment` INT,`series_title_temp` string,`title_series_title` string,`article_type` INT,`recommend` INT,`recommend_display_time` string,`export_from` string,`transfer` INT,`reward_count` INT,`hash_value` string,`flagfield` string,`sync_date` string) LOCATION "/recommend/dw/sync_yuanchuang";
CREATE EXTERNAL TABLE IF NOT EXISTS `sync_youhui` (`id` INT,`editor_id` MEDIUMINT,`pubdate` string,`choiceness_date` string,`yh_status` INT,`channel` INT,`comment_count` INT,`collection_count` INT,`praise` INT,`sum_collect_comment` INT,`mall` string,`brand` string,`digital_price` string,`worthy` INT,`unworthy` INT,`is_top` INT,`yh_type` string,`is_essence_for_editor` INT,`article_type` INT,`mobile_exclusive` INT,`clean_link` string,`district` INT,`is_review` INT,`faxian_show` INT,`source_from` INT,`strategy_pub` INT,`uhomedate` string,`update_timestamp` string,`reward_count` INT,`mall_id` string,`brand_id` string,`b2c_id` string,`spu_link` string,`hash_value` string,`flagfield` string,`sync_date` string) LOCATION "/recommend/dw/sync_youhui";
CREATE EXTERNAL TABLE IF NOT EXISTS `sync_youhui_extend` (`id` INT,`createdate` string,`upstring` string,`title_prefix` string,`title` string,`subtitle` string,`phrase_desc` string,`content` mediumstring,`focus_pic_url` string,`referrals` string,`direct_link` string,`direct_link_name` string,`direct_link_list` string,`sales_area` MEDIUMINT,`title_mode` INT,`app_push` INT,`last_editor_id` MEDIUMINT,`sync_home_id` string,`source_from_id` string,`source_from_channel` INT,`sina_id` string,`associate_brand` INT,`associate_mall` INT,`is_anonymous` INT,`stock_status` INT,`comment_switch` INT,`push_type` INT,`guonei_id_for_fx` string,`haitao_id_for_fx` string,`sync_home` INT,`sync_home_time` string,`is_home_top` INT,`edit_page_type` INT,`hash_value` string,`flagfield` string,`sync_date` string,`starttime` string,`endtime` string) LOCATION "/recommend/dw/sync_youhui_extend";
CREATE EXTERNAL TABLE IF NOT EXISTS `sync_yuanchuang_extend` (`id` INT,`edit_uid` INT,`updateline` string,`dateline` string,`submit_time` string,`last_submit_time` string,`audit_times` INT,`sina_id` string,`tencent_id` string,`remark` string,`plid` string,`district` INT,`have_read` INT,`add_modify` string,`add_modify_time` string,`seo_title` string,`seo_keywords` string,`seo_description` string,`series_id` INT,`series_order_id` INT,`push_type` INT,`baidu_doc_id` string,`probreport_id` string,`is_write_post_time` string,`set_auto_sync` INT,`is_home_top` INT,`from_vote` string,`comment_switch` INT,`associate_brand` INT,`associate_mall` INT,`hash_value` string,`flagfield` string,`sync_date` string) LOCATION "/recommend/dw/sync_yuanchuang_extend";'

'jdbc:mysql://smzdm_recommend_mysql_m01_184/recommendDB?zeroDateTimeBehavior=convertToNull?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false' --username recommendUser --password pVhXTntx9ZG --query "select * from sync_youhui where pubdate>ctime(sync_time) \$CONDITIONS " --split-by id -m 4 --fields-terminated-by '\001' --null-string '' --null-non-string '' --hive-drop-import-delims --as-textfile --delete-target-dir --target-dir /recommend/dw/sync_youhui --hive-table recommend.sync_youhui> $home_path/file/log/sync_youhui.log 2>&1
