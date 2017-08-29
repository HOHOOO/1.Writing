

# 创建数据表结构
```sh
CREATE TABLE `home_article_online` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'pk',
  `article_id` bigint(20) NOT NULL DEFAULT '0' COMMENT '文章id',
  `channel_id` tinyint(4) NOT NULL DEFAULT '0' COMMENT 'yh: 3, zx: 6, pc: 7, yc: 11, wk: 14, xr: 31, sp: 38',
  `channel` varchar(4) NOT NULL DEFAULT '' COMMENT 'yh: 优惠, zx: 资讯, pc: 评测, yc: 原创, wk: wiki, xr: 新锐品牌, sp: 视频',
  `level1_ids` varchar(50) NOT NULL DEFAULT '' COMMENT '一级品类列表',
  `level2_ids` varchar(1000) NOT NULL DEFAULT '' COMMENT '二级品类列表',
  `level3_ids` varchar(1000) NOT NULL DEFAULT '' COMMENT '三级品类列表',
  `level4_ids` varchar(1000) NOT NULL DEFAULT '' COMMENT '四级品类列表',
  `tag_ids` varchar(1000) NOT NULL DEFAULT '' COMMENT '标签id列表',
  `brand_ids` varchar(1000) NOT NULL DEFAULT '' COMMENT '品牌id列表',
  `sync_home` tinyint(4) NOT NULL DEFAULT '0' COMMENT '0不同步，1立即同步，2定时同步 ',
  `is_top` tinyint(2) NOT NULL DEFAULT '0' COMMENT '同步是否置顶',
  `machine_report` tinyint(4) NOT NULL DEFAULT '0' COMMENT '0:涨价,1:降价,2:售罄',
  `publish_time` datetime NOT NULL DEFAULT '0000-00-00 00:00:00' COMMENT '发布时间',
  `sync_home_time` datetime NOT NULL DEFAULT '0000-00-00 00:00:00' COMMENT '同步主站首页的时间',
  `sync_time` datetime NOT NULL DEFAULT '0000-00-00 00:00:00' COMMENT '同步时间',
  `auto_updatetime` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '自动记录更新时间',
  `status` tinyint(2) NOT NULL DEFAULT '0' COMMENT '0: 正常数据 1: 异常或者删除数据',
  `title` varchar(500) DEFAULT '' COMMENT '文章标题',
  `comment_count` int(11) NOT NULL DEFAULT '0' COMMENT '评论数',
  `collection_count` int(11) NOT NULL DEFAULT '0' COMMENT '收藏数',
  `praise` int(10) NOT NULL DEFAULT '0' COMMENT '赞',
  `sum_collect_comment` int(11) NOT NULL DEFAULT '0' COMMENT '热度>=评论数+收藏数+赞数',
  `mall` varchar(200) NOT NULL DEFAULT '' COMMENT '商城',
  `brand` varchar(200) NOT NULL DEFAULT '' COMMENT '品牌',
  `digital_price` varchar(50) NOT NULL DEFAULT '' COMMENT '数字价格',
  `worthy` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '值的数量',
  `unworthy` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '不值的数量',
  `mall_id` bigint(20) NOT NULL DEFAULT '0' COMMENT '商城id',
  `article_channel` varchar(20) NOT NULL DEFAULT '' COMMENT '文章频道id组合串',
  `is_delete` tinyint(2) NOT NULL DEFAULT '0' COMMENT '0: 正常数据, 1:小编取消同步到首页, 2: 推荐已经同步到首页',
  PRIMARY KEY (`id`),
  UNIQUE KEY `article_id_channel_unique_key` (`article_id`,`channel`),
  KEY `article_id_channel` (`article_id`,`channel`) USING BTREE,
  KEY `publish_time` (`publish_time`),
  KEY `sync_home_time` (`sync_home_time`),
  KEY `sync_time` (`sync_time`),
  KEY `auto_updatetime` (`auto_updatetime`)
) ENGINE=InnoDB AUTO_INCREMENT=74902 DEFAULT CHARSET=utf8 COMMENT='首页编辑精选数据同步表' |

```

# 同步逻辑

```sh


# ip/host
10.10.177.37 test158_dbzdm_youhui
10.10.165.251 test158_smzdm_yuanchuang
10.10.183.62 test158_smzdm
10.10.165.251 test158_smzdm_probation
10.9.144.150 test158_newbrand
10.9.185.235 test158_dbzdm_video

# 优惠
mysql -htest158_dbzdm_youhui -uyouhui_user -pxlJ7Enmhs -P3403 dbzdm_youhui

# 原创
mysql -htest158_smzdm_probation -usmzdm_post -psmzdmPost_162304 -P3306 smzdm_yuanchuang

# 评测
mysql -htest158_smzdm_probation -usmzdm -psMzdmTest -P3306 smzdm_probation

# 资讯
mysql -htest158_smzdm -usmzdm -psMzdmTest smzdm

# 新锐品牌
mysql -htest158_newbrand -unewbrandUser -pe8dgHs3vmp -P3306 newbrandDB

# 视频
mysql -htest158_dbzdm_video -uvideo_user -pa8dfdEdsa03fdse9 -P3306 dbzdm_video

# 首页编辑精选
mysql -htest158_smzdm_mysql_homepage -uhomepageUser -puUVer19Jd5D -P3306 homepageDB  # test
mysql -hsmzdm_mysql_homepage -uhomepageUser -puUVer19Jd5D -P3306 homepageDB  # online

# 需要同步的数据源
channel=3 优惠 （已确定）
channel=6 资讯 （会细分其它类别）
channel=7 评测
channel=11 原创
channel=14 wiki
channel=31 新锐品牌
channel=38 视频
channel=48 直播

# 需要同步的字段
文章id，频道id，四级品类，标签id，品牌id，发布时间，是否同步到首页, 同步到首页时间, 同步到首页是否置顶，同步到首页是否删除


### 原创 ---孙运剑
# 增量同步
1. 新数据按着yuanchuang表中的publishtime发布时间获取
2. 历史文章有更新的，按着yuanchuang_extend表中的updateline字段获取

# 文章id，发布时间
select id, publishtime  from yuanchuang where type in (3,8) and publishtime >= '2017-05-03 12:00:00' and publishtime < '2017-05-03 01:00:00' and is_delete = 0
# 品牌id
select brand_id from yuanchuang_brand_item where blog_id=541669
# 四级品类
select level_first,level_second,level_third,level_four from yuanchuang_category_relation where yc_id = 541669
# 标签id
select tag_id from yuanchuang_tag_item where blog_id=541669

# 是否同步到首页，同步到首页时间，同步到首页是否置顶
select is_write_post_time,set_auto_sync,is_home_top from yuanchuang_extend where id = 7208331\G;


### 优惠 ---相明亮
# 增量同步
1. 新数据按着youhui表中的choiceness_date发布时间获取，例如：
select id,choiceness_date,brand_id,channel from youhui where yh_status = 1 and channel in (1, 5)  and choiceness_date >= '2017-05-03 03:00:00' and choiceness_date  < '2017-05-03 04:00:00';
2. 历史文章有更新的，按着youhui_extend表中的updatetime字段获取，例如：
select id from youhui_extend where updatetime >= '2017-05-03 03:00:00' and updatetime  < '2017-05-03 04:00:00';

# 各个字段获取
1. 文章id， 发布时间, 品牌id (只取国内1和海淘5)
select id,choiceness_date,brand_id,channel from youhui where yh_status = 1 and channel in (1, 5)  and choiceness_date > '2017-05-03 12:00:00' limit 2\G;
2. 四级品类
select top_category_id,second_category_id,third_category_id,fourth_category_id from youhui_category where youhui_id = 7208331\G;
3. 标签id
select tag_id from youhui_tag_type_item where article_id=7208331 and channel_id = 1 \G;
4. 是否同步到首页，同步到首页时间，同步到首页是否置顶
select sync_home,sync_home_time,is_home_top from youhui_extend where id = 7208331\G;


### 资讯 ---孙运剑
# 数据增量同步
1. 新增数据同步
按着smzdm_news表中的news_pub_date发布时间获取。 例如：同步2017-05-04 12:00:00至2017-05-04 01:00:00文章
select id, news_pub_date from smzdm_news where news_pub_date >= '2017-05-04 12:00:00' and news_pub_date < '2017-05-04 01:00:00' and news_status = 1

2. 更新数据同步
按着smzdm_news表中的news_date字段获取。例如：同步2017-05-04 12:00:00至2017-05-04 01:00:00的更新信息
select id, news_pub_date from smzdm_news where news_date >= '2017-05-04 12:00:00' and news_date < '2017-05-04 01:00:00' and news_status = 1

3. 编辑首页状态同步
按着smzdm_news表中的is_write_post_time字段获取。例如：同步编辑在2017-05-04 12:00:00至2017-05-04 01:00:00时间段内同步到首页的资讯文章
select is_write_post,set_auto_sync,is_write_post_time,is_home_top from smzdm_news where is_write_post_time >= '2017-05-04 12:00:00' and is_write_post_time < '2017-05-04 01:00:00' and news_status = 1

# 各个字段获取
1. 文章id， 发布时间
select id, news_pub_date from smzdm_news where news_date >= '2017-05-04 12:00:00' and news_date < '2017-05-04 01:00:00' and news_status = 1;

2. 品牌id
select brand_id from smzdm_brand_item where blog_id=article_id and type=6

3. 四级品类
还未上线，在158上可以查smzdm_news_category_relation表

4. 标签id
select tag_id from smzdm_tag_type_item where blog_id=5481 and type = 6

5. 是否同步到首页，同步到首页时间，同步到首页是否置顶
select is_write_post,set_auto_sync,is_write_post_time,is_home_top from smzdm_news where id = 947\G;


### 评测---徐学勇
# 登录
mysql -hdb-server_read09_eth01 -usmzdm -psMzdmTest -P3306 smzdm_probation

# 数据增量同步
1. 新增数据同步
按着zdmdb_probreport表中的publishtime发布时间获取。 例如：同步2017-05-04 12:00:00至2017-05-04 01:00:00文章
select id, publishtime from zdmdb_probreport where publishtime >= '2017-05-04 12:00:00' and publishtime < '2017-05-04 01:00:00' and type = 3 and is_delete= 0

2. 更新数据同步
按着zdmdb_probreport表中的updateline最后更新字段获取。例如：同步2017-05-04 12:00:00至2017-05-04 01:00:00的更新信息
select id, publishtime from zdmdb_probreport where updateline >= '2017-05-04 12:00:00' and updateline < '2017-05-04 01:00:00' and type = 3 and is_delete= 0

3. 编辑首页状态同步
按着zdmdb_probreport表中的is_write_post_time字段获取。例如：同步编辑在2017-05-04 12:00:00至2017-05-04 01:00:00时间段内同步到首页的资讯文章
select is_write_post,set_auto_sync,is_write_post_time,is_home_top from zdmdb_probreport where is_write_post_time >= '2017-05-04 12:00:00' and is_write_post_time < '2017-05-04 01:00:00' and type = 3 and is_delete= 0

# 各个字段获取
1. 文章id， 发布时间, 品牌id(评测库中后面新增加的可能不准确),品牌名称(准确)
select id, publishtime,brand_id,brand from zdmdb_probreport where publishtime >= '2017-05-04 12:00:00' and publishtime < '2017-05-04 01:00:00' and type = 3 and is_delete= 0 limit 1\G;

2. 四级品类
select cateid from zdmdb_probreport_category  where pbrtid=article_id
/**
 * 根据多个活动id获取活动信息
 * @param $ids string 多个活动id 123,456,789
 * @param array $option
 * @return array 多条数据
 * @author sunyunjian
 * @time 2015-08-14 11:25:00
 */
public function get_data_by_ids($ids,$option=[]){
    $bind_params = explode(',',$ids);
    $ids_sql = $this->smzdm_db->get_in_sql($bind_params);
    $sql="SELECT ID,parent_ids,title,parent_id,url_nicktitle
          FROM {$this->table}
          WHERE ID IN ({$ids_sql}) AND is_deleted=?";
    array_push($bind_params,0);
    // $this->smzdm_db->debug(1);
    $result=$this->smzdm_db->prepare_query($sql,$bind_params);
    return empty($result)?array():$result;
}

3. 标签id(在smzdm的库中)
select tag_id from smzdm.smzdm_tag_type_item where blog_id=article_id and type =7;

4. 是否同步到首页，同步到首页时间，同步到首页是否置顶
select is_write_post,set_auto_sync,is_write_post_time,is_home_top from zdmdb_probreport where is_write_post_time >= '2017-05-04 12:00:00' and is_write_post_time < '2017-05-04 01:00:00' and type = 3 and is_delete= 0 and is_write_post = 1 \G;


### 新锐品牌
1. 新增数据同步
按着brand_special表中的pub_time发布时间获取。 例如：同步2017-05-04 12:00:00至2017-05-04 01:00:00文章
select id, pub_time from brand_special where pub_time >= '2017-05-04 12:00:00' and pub_time < '2017-05-04 01:00:00' and status = 1 and push_home =1

2. 更新数据同步
按着brand_special表中的editdate最后更新字段获取。例如：同步2017-05-04 12:00:00至2017-05-04 01:00:00的更新信息
select id, pub_time from brand_special where editdate >= '2017-05-04 12:00:00' and editdate < '2017-05-04 01:00:00' and status = 1 and push_home =1


# 各个字段获取
1. 文章id， 发布时间
select id, pub_time from brand_special where pub_time >= '2017-05-04 12:00:00' and pub_time < '2017-05-04 01:00:00' and status = 1 and push_home =1

2. 四级品类
无

3. 品牌id
select brand_id from brand_special_item where item_id = 103\G;

4. 标签id
无

5. 是否同步到首页，同步到首页时间，同步到首页是否置顶
select push_home from brand_special where pub_time >= '2017-05-04 12:00:00' and pub_time < '2017-05-04 01:00:00' and status = 1 and push_home =1


### wiki
# 登录
mysql -hwiki_db-server_read02_eth01  -uproductAdmin -pproductTest product
mysql -hwiki_mysql_01  -uproductAdmin -pproductTest product

1. 新增数据同步
按着topic表中的publish_date获取
2. 更新数据同步
按着topic表中的modify_date获取


### 视频---陈大程
1. 新增数据同步
按着v_video表中的publish_date发布时间获取。 例如：同步2017-05-04 12:00:00至2017-05-04 01:00:00文章
select id, publish_date from v_video where publish_date >= '2017-05-04 12:00:00' and publish_date < '2017-05-04 01:00:00' and status = 3 and is_delete = 0

2. 更新数据同步
按着v_video表中的modification_date最后更新字段获取。例如：同步2017-05-04 12:00:00至2017-05-04 01:00:00的更新信息
select id, publish_date from v_video where modification_date >= '2017-05-04 12:00:00' and modification_date < '2017-05-04 01:00:00' and status = 3 and is_delete = 0


# 各个字段获取
1. 文章id， 发布时间
select id, publish_date from v_video where publish_date >= '2017-05-04 12:00:00' and publish_date < '2017-05-04 01:00:00' and status = 3 and is_delete = 0

2. 四级品类(视频特有的2级分类)
无

3. 品牌id
select brand_id from v_brand_item where video_id=articleid;

4. 标签id
select tag_id from v_tag_item where video_id=articleid;

5. 是否同步到首页，同步到首页时间，同步到首页是否置顶
select set_auto_sync,is_write_post_time,is_home_top from v_video_extend where video_id=article_id

```

# 表结构建立
```sh
# youhui

# 测试数据导出
CREATE TABLE `youhui` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '文章id',
  `editor_id` mediumint(11) DEFAULT '0' COMMENT '小编id（稿件创建者）',
  `pubdate` datetime NOT NULL DEFAULT '0000-00-00 00:00:00' COMMENT '发布时间',
  `choiceness_date` datetime NOT NULL DEFAULT '0000-00-00 00:00:00' COMMENT '精选时间',
  `yh_status` tinyint(4) NOT NULL DEFAULT '3' COMMENT '文章状态（ 1：发布；2：回收站 3:草稿,；）',
  `channel` int(1) DEFAULT NULL COMMENT '频道',
  `comment_count` int(11) NOT NULL DEFAULT '0' COMMENT '评论数',
  `collection_count` int(11) NOT NULL DEFAULT '0' COMMENT '收藏数',
  `praise` int(10) NOT NULL DEFAULT '0' COMMENT '赞',
  `sum_collect_comment` int(11) NOT NULL DEFAULT '0' COMMENT '热度>=评论数+收藏数+赞数',
  `mall` varchar(200) NOT NULL DEFAULT '' COMMENT '商城',
  `brand` varchar(200) NOT NULL DEFAULT '' COMMENT '品牌',
  `digital_price` varchar(50) NOT NULL DEFAULT '' COMMENT '数字价格',
  `worthy` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '值的数量',
  `unworthy` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '不值的数量',
  `is_top` tinyint(4) NOT NULL DEFAULT '0' COMMENT '是否当前频道置顶，0非置顶，1置顶',
  `yh_type` varchar(50) NOT NULL DEFAULT '' COMMENT '日志类型',
  `is_essence_for_editor` tinyint(4) NOT NULL DEFAULT '0' COMMENT '是否精华文章。给编辑区分优惠的好坏使用。0非精华，1精华',
  `article_type` tinyint(4) NOT NULL DEFAULT '0' COMMENT '0无状态 1发现 2精选 3个人中心',
  `mobile_exclusive` tinyint(4) NOT NULL DEFAULT '0' COMMENT '是否是移动专享。0无专享，1是移动专享，2微信专享',
  `clean_link` varchar(600) NOT NULL DEFAULT '' COMMENT '干净链接',
  `district` int(1) NOT NULL DEFAULT '1' COMMENT '1是国内    2是海淘 3跨境',
  `is_review` int(1) NOT NULL DEFAULT '0' COMMENT '0策略发布已读 已处理 或者非策略发布 1策略发布未读，未处理',
  `faxian_show` int(1) NOT NULL DEFAULT '1' COMMENT '发现是否显示0不显示 1显示',
  `source_from` tinyint(4) NOT NULL DEFAULT '1' COMMENT '优惠来源 1编辑自建2用户爆料3降价榜采集4秒杀5商家爆料6竞品采集7商务推广8淘宝C店',
  `strategy_pub` tinyint(4) DEFAULT '0' COMMENT '0 非策略发布 1策略发布 2初筛发布个人主页',
  `uhomedate` datetime NOT NULL DEFAULT '0000-00-00 00:00:00' COMMENT '发布个人主页时间',
  `reward_count` int(11) NOT NULL DEFAULT '0' COMMENT '打赏次数',
  `mall_id` bigint(20) NOT NULL DEFAULT '0' COMMENT '商城id',
  `brand_id` bigint(20) NOT NULL DEFAULT '0' COMMENT '品牌id',
  `update_timestamp` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
  `b2c_id` bigint(20) NOT NULL COMMENT 'b2c 店中店id',
  `spu_link` varchar(600) NOT NULL DEFAULT '' COMMENT '父级干净链接',
  `auto_updatetime` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'è‡ªåŠ¨è®°å½•æ›´æ–°æ—¶é—´',
  `no_protocol_link` varchar(600) NOT NULL DEFAULT '' COMMENT '无协议链接',
  PRIMARY KEY (`id`),
  KEY `index_clean_link` (`clean_link`(255)) USING BTREE,
  KEY `index_status_date` (`channel`,`yh_status`,`pubdate`) USING BTREE,
  KEY `index_mall_date` (`channel`,`mall`,`pubdate`) USING BTREE,
  KEY `index_type` (`channel`,`yh_type`,`pubdate`) USING BTREE,
  KEY `index_faxian_show` (`channel`,`yh_status`,`faxian_show`),
  KEY `pubdate` (`pubdate`),
  KEY `index_choiceness_date` (`channel`,`yh_status`,`choiceness_date`),
  KEY `uhomedate` (`uhomedate`)
) ENGINE=InnoDB AUTO_INCREMENT=6038537 DEFAULT CHARSET=utf8;

CREATE TABLE `youhui_extend` (
  `id` bigint(20) unsigned NOT NULL COMMENT '文章id',
  `createdate` datetime NOT NULL DEFAULT '0000-00-00 00:00:00' COMMENT '创建时间',
  `updatetime` datetime NOT NULL DEFAULT '0000-00-00 00:00:00' COMMENT '更新时间',
  `title_prefix` varchar(50) NOT NULL DEFAULT '' COMMENT '标题前缀',
  `title` varchar(1000) NOT NULL DEFAULT '' COMMENT '优惠标题',
  `subtitle` varchar(200) NOT NULL DEFAULT '' COMMENT '副标题，原标题价格',
  `phrase_desc` varchar(1000) NOT NULL DEFAULT '' COMMENT '优惠力度，一句话描述',
  `content` mediumtext COMMENT '优惠内容',
  `focus_pic_url` text COMMENT '焦点图地址（图片，宽高 序列化后存储）',
  `referrals` varchar(200) NOT NULL DEFAULT '' COMMENT '爆料人',
  `direct_link` text COMMENT '直达链接',
  `direct_link_name` varchar(1000) DEFAULT NULL COMMENT '直达链接title',
  `direct_link_list` text COMMENT '多个直达链接',
  `sales_area` mediumint(9) NOT NULL DEFAULT '0' COMMENT '购买地域对应 smzdm_buy_domain表的ID',
  `title_mode` tinyint(4) NOT NULL DEFAULT '0' COMMENT '是否是标题模式,0不是，1是标题模式',
  `app_push` tinyint(4) NOT NULL DEFAULT '0' COMMENT '客户端是否推送过，0没推送，1已推送',
  `last_editor_id` mediumint(8) unsigned NOT NULL DEFAULT '0' COMMENT '最后操作的编辑id',
  `sync_home_id` bigint(20) unsigned NOT NULL DEFAULT '0' COMMENT '同步到主站首页的id',
  `source_from_id` bigint(20) NOT NULL DEFAULT '0' COMMENT '信息来源的文章id，例如发现转过来的的就是发现的id',
  `source_from_channel` tinyint(4) NOT NULL DEFAULT '0' COMMENT '信息来源的频道id，1优惠，2发现，5海淘',
  `sina_id` varchar(30) NOT NULL DEFAULT '0' COMMENT '新浪微博id',
  `associate_brand` tinyint(4) NOT NULL DEFAULT '0' COMMENT '是否关联品牌库，0未关联，1已关联',
  `associate_mall` tinyint(4) NOT NULL DEFAULT '0' COMMENT '是否关联商家库，0未关联，1已关联',
  `is_anonymous` tinyint(4) NOT NULL DEFAULT '0' COMMENT '是否匿名。0不匿名，1匿名',
  `stock_status` tinyint(4) NOT NULL DEFAULT '0' COMMENT '是否已售罄，0正常，1已过期，2已售罄',
  `comment_switch` tinyint(4) NOT NULL DEFAULT '1' COMMENT '评论开关，1开启 0关闭',
  `push_type` tinyint(4) NOT NULL DEFAULT '1' COMMENT '推送类型。0不推送，1普通推送，2每日精选推送、3强制推送',
  `guonei_id_for_fx` bigint(20) NOT NULL DEFAULT '0' COMMENT '原发现同步到优惠后的优惠文章id。发现表使用',
  `haitao_id_for_fx` bigint(20) DEFAULT '0' COMMENT '原发现同步到海淘后的海淘文章id。发现表使用',
  `sync_home` tinyint(4) NOT NULL DEFAULT '0' COMMENT '是否同步到主站首页，0不同步，1立即同步，2定时同步 ',
  `sync_home_time` datetime NOT NULL DEFAULT '0000-00-00 00:00:00' COMMENT '同步主站首页的时间',
  `is_home_top` tinyint(4) NOT NULL DEFAULT '0' COMMENT '是否主站首页置顶，0不置顶，1置顶',
  `edit_page_type` tinyint(4) NOT NULL DEFAULT '1' COMMENT '编辑页类型。1专业型，2通用型',
  `starttime` datetime DEFAULT '0000-00-00 00:00:00' COMMENT '优惠开始时间',
  `endtime` datetime DEFAULT '0000-00-00 00:00:00' COMMENT '优惠结束时间',
  `b2c_name` varchar(255) DEFAULT NULL COMMENT 'b2c_商城名',
  PRIMARY KEY (`id`),
  KEY `source_from_id` (`source_from_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `youhui_tag_type_item` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `tag_id` bigint(11) NOT NULL DEFAULT '0' COMMENT '标签id',
  `article_id` bigint(11) DEFAULT '0' COMMENT '文章id',
  `channel_id` tinyint(2) NOT NULL DEFAULT '0' COMMENT '频道类型。1优惠，5海淘（导数据需要颠倒一下）',
  PRIMARY KEY (`id`),
  UNIQUE KEY `tag_index` (`tag_id`,`article_id`),
  KEY `article_index` (`article_id`)
) ENGINE=InnoDB AUTO_INCREMENT=3099815 DEFAULT CHARSET=utf8;

CREATE TABLE `youhui_category` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `youhui_id` bigint(20) NOT NULL DEFAULT '0' COMMENT '优惠id',
  `top_category_id` int(11) NOT NULL DEFAULT '0' COMMENT '一级分类id',
  `second_category_id` int(11) NOT NULL DEFAULT '0' COMMENT '二级分类id',
  `third_category_id` int(11) NOT NULL DEFAULT '0' COMMENT '三级分类id',
  `fourth_category_id` int(11) NOT NULL COMMENT '四级分类id',
  PRIMARY KEY (`id`),
  UNIQUE KEY `index_youhui_id` (`youhui_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=616188 DEFAULT CHARSET=utf8;

# yuanchuang
 CREATE TABLE `yuanchuang` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `uid` int(10) NOT NULL,
  `anonymous` tinyint(1) NOT NULL DEFAULT '0',
  `type` tinyint(1) NOT NULL DEFAULT '0' COMMENT '0:草稿,1:正常待审核,3:通过,4:待修改,5:审核未通过 6.审核中,8.发布到个人主页，9.已删除，10.搜索时候全部，13.即将发布，14.未读回复，16.即将发布 个人主页，17新提交未审核',
  `title` varchar(255) NOT NULL DEFAULT '',
  `image` varchar(255) NOT NULL DEFAULT '' COMMENT '主图',
  `comment_count` int(9) DEFAULT '0' COMMENT '评论数',
  `collection_count` int(9) DEFAULT '0' COMMENT '喜欢数',
  `love_rating_count` int(11) DEFAULT '0' COMMENT '喜欢数',
  `reward_count` int(11) DEFAULT '0' COMMENT '打赏总数',
  `brand` varchar(255) NOT NULL DEFAULT '',
  `link` text,
  `mall` varchar(255) NOT NULL DEFAULT '',
  `is_delete` tinyint(1) NOT NULL DEFAULT '0',
  `status` tinyint(2) unsigned NOT NULL DEFAULT '0' COMMENT '0正常,1追加修改中,2追加已发布,3追加待审核,4追加审核中，5.追加审核未通过，6.追加审核初次驳回',
  `publishtime` datetime NOT NULL DEFAULT '0000-00-00 00:00:00' COMMENT '发布时间',
  `sum_collect_comment` int(11) NOT NULL DEFAULT '0' COMMENT '每次喜欢、每次评论都会增加    例如 3_2324323',
  `series_title_temp` varchar(255) NOT NULL DEFAULT '' COMMENT '为草稿临时放置文章系列标题',
  `title_series_title` varchar(255) NOT NULL DEFAULT '' COMMENT '主标题和副标题集合为了搜索',
  `article_type` tinyint(4) NOT NULL DEFAULT '0' COMMENT '文章类别。1开箱晒物，2试用评测,3消费知识，4生活记录，5购物攻略，6摄影旅游',
  `recommend` tinyint(4) NOT NULL DEFAULT '0' COMMENT '推荐到频道首页，0是不推荐，1是推荐',
  `recommend_display_time` datetime NOT NULL DEFAULT '0000-00-00 00:00:00' COMMENT '推荐显示时间，首页推荐需要在推荐显示时间到了才显示',
  `export_from` varchar(50) NOT NULL DEFAULT '' COMMENT '从哪里导入的，原创频道上线时导数据临时使用',
  `transfer` tinyint(1) NOT NULL DEFAULT '0' COMMENT '作者是否允许转载，0：允许，1：不允许',
  `auto_updatetime` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'è‡ªåŠ¨è®°å½•æ›´æ–°æ—¶é—´',
  `trial` tinyint(4) DEFAULT '0' COMMENT '是否是先发后审的数据',
  `review` tinyint(4) DEFAULT '0' COMMENT '是否复审',
  `audit_id` int(10) unsigned DEFAULT NULL COMMENT '首次打开该文章的审核人id',
  PRIMARY KEY (`id`),
  KEY `uid` (`uid`),
  KEY `arttype_t_pt_index` (`article_type`,`type`,`publishtime`) USING BTREE,
  KEY `t_re_pt_index` (`type`,`recommend_display_time`,`publishtime`) USING BTREE,
  KEY `publishtime` (`publishtime`,`recommend`,`type`,`is_delete`)
) ENGINE=InnoDB AUTO_INCREMENT=476223 DEFAULT CHARSET=utf8 COMMENT='原创主表';

CREATE TABLE `yuanchuang_extend` (
  `id` int(10) unsigned NOT NULL,
  `content1` longtext CHARACTER SET utf8mb4 COMMENT '',
  `content2` longtext CHARACTER SET utf8mb4 COMMENT '' DEFAULT '0',
  `remark` text COMMENT '用户备注信息',
  `plid` bigint(20) NOT NULL DEFAULT '0' COMMENT '存储ucenter中的会话id',
  `district` tinyint(1) NOT NULL DEFAULT '0' COMMENT '域地 0 国内 1海淘（默认国内）',
  `have_read` tinyint(1) NOT NULL DEFAULT '0' COMMENT '是否已读 0已读 1未读',
  `add_modify` longtext COMMENT '追加修改内容',
  `add_modify_time` datetime NOT NULL DEFAULT '0000-00-00 00:00:00' COMMENT '追加修改时间',
  `seo_title` varchar(255) NOT NULL DEFAULT '' COMMENT 'seo标题',
  `seo_keywords` varchar(255) NOT NULL DEFAULT '' COMMENT 'seo关键词',
  `seo_description` varchar(500) NOT NULL DEFAULT '' COMMENT 'seo页面描述',
  `series_id` int(10) NOT NULL DEFAULT '0' COMMENT '系列文章ID',
  `series_order_id` int(5) NOT NULL DEFAULT '0' COMMENT '系列序号ID',
  `push_type` tinyint(1) NOT NULL DEFAULT '1' COMMENT '0不推送，1普通推送，2每日精选推送、3强制推送',
  `baidu_doc_id` varchar(100) NOT NULL DEFAULT '0' COMMENT '百度文库id',
  `probreport_id` bigint(20) unsigned NOT NULL DEFAULT '0' COMMENT '关联众测id',
  `is_write_post_time` datetime NOT NULL DEFAULT '0000-00-00 00:00:00' COMMENT '定时同步到主站。与set_auto_sync配合使用，作为立即同步或者定时同步的时间',
  `set_auto_sync` tinyint(1) NOT NULL DEFAULT '0' COMMENT '设置自动同步到主站标识，1是立即同步主站，2是定时同步主站',
  `is_home_top` tinyint(1) NOT NULL DEFAULT '0' COMMENT '同步到主站是否置顶',
  `from_vote` bigint(20) unsigned NOT NULL DEFAULT '0' COMMENT '来自征稿vote_id，默认为0',
  `comment_switch` tinyint(4) NOT NULL DEFAULT '1' COMMENT '评论开关，1开启 0关闭',
  `associate_brand` tinyint(3) unsigned NOT NULL DEFAULT '0' COMMENT '是否关联品牌库，0未关联，1已关联',
  `associate_mall` tinyint(3) unsigned NOT NULL DEFAULT '0' COMMENT '是否关联商家库，0未关联，1已关联',
  `review_date` datetime NOT NULL DEFAULT '0000-00-00 00:00:00' COMMENT '复审时间',
  PRIMARY KEY (`id`),
  KEY `probreport_id` (`probreport_id`) USING BTREE,
  KEY `series_id` (`series_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='原创主表';

CREATE TABLE `yuanchuang_brand_item` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `brand_id` int(11) NOT NULL DEFAULT '0' COMMENT '品牌id',
  `blog_id` int(11) NOT NULL DEFAULT '0' COMMENT '日志id',
  PRIMARY KEY (`id`),
  UNIQUE KEY `blog_brand_index` (`blog_id`,`brand_id`),
  KEY `brand_id_index` (`brand_id`)
) ENGINE=InnoDB AUTO_INCREMENT=107650 DEFAULT CHARSET=utf8 COMMENT='原创品牌关联表' ;

CREATE TABLE `yuanchuang_category_relation` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `yc_id` bigint(20) NOT NULL DEFAULT '0' COMMENT '晒单id',
  `level_first` int(4) NOT NULL DEFAULT '0' COMMENT '一级分类id',
  `level_second` int(4) NOT NULL DEFAULT '0' COMMENT '二级分类id',
  `level_third` int(4) NOT NULL DEFAULT '0' COMMENT '三级分类id',
  `level_four` int(4) NOT NULL DEFAULT '0' COMMENT '四级分类id',
  PRIMARY KEY (`id`),
  KEY `osid` (`yc_id`,`level_first`,`level_second`,`level_third`,`level_four`)
) ENGINE=InnoDB AUTO_INCREMENT=395109 DEFAULT CHARSET=utf8 COMMENT='原创分类关联关系详细表（数据组单独使用）'   ;


CREATE TABLE `yuanchuang_tag_item` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `tag_id` int(11) DEFAULT '0' COMMENT '标签id',
  `blog_id` int(11) DEFAULT '0' COMMENT '日志id',
  `type` tinyint(4) DEFAULT '0' COMMENT '标签来源',
  PRIMARY KEY (`id`),
  UNIQUE KEY `tag_blog_index` (`blog_id`,`tag_id`),
  KEY `tag_id_index` (`tag_id`)
) ENGINE=InnoDB AUTO_INCREMENT=257284 DEFAULT CHARSET=utf8 COMMENT='原创tag关联表'


# zixun
CREATE TABLE `smzdm_tag_type_item` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `tag_id` int(11) DEFAULT NULL,
  `blog_id` int(11) DEFAULT '0' COMMENT '日志id',
  `type` smallint(2) NOT NULL DEFAULT '0' COMMENT '1,海淘',
  PRIMARY KEY (`id`),
  UNIQUE KEY `index_blog_tag` (`type`,`blog_id`,`tag_id`),
  KEY `tag_combined_index` (`tag_id`,`type`),
  KEY `blog_combined_index` (`blog_id`,`type`)
) ENGINE=MyISAM AUTO_INCREMENT=6734614 DEFAULT CHARSET=utf8;

CREATE TABLE `smzdm_brand_item` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `brand_id` int(11) DEFAULT NULL,
  `blog_id` int(11) DEFAULT '0' COMMENT '日志id',
  `type` smallint(2) NOT NULL DEFAULT '0' COMMENT '1,优惠；2,发现；3,晒物；4,经验；5,海淘；6,新闻；',
  PRIMARY KEY (`id`),
  UNIQUE KEY `combined_index` (`brand_id`,`type`,`blog_id`) USING BTREE,
  KEY `blog_combined_index` (`blog_id`,`type`)
) ENGINE=InnoDB AUTO_INCREMENT=845564 DEFAULT CHARSET=utf8;

CREATE TABLE `smzdm_news` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `news_author` bigint(20) unsigned NOT NULL COMMENT 'bgm管理用户ID 发布人id',
  `news_date` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  `news_title` varchar(200) NOT NULL,
  `title_height_light` varchar(200) NOT NULL COMMENT '标题高亮',
  `news_content` longtext NOT NULL,
  `news_pic_url` text,
  `news_status` int(1) NOT NULL DEFAULT '0' COMMENT ' 1：发布；2：回收站 3:草稿 10：预发布 11，空草稿；',
  `news_comment_count` int(11) unsigned NOT NULL DEFAULT '0',
  `collection_count` int(11) unsigned NOT NULL DEFAULT '0',
  `news_love_count` int(11) DEFAULT '0' COMMENT '喜欢数',
  `news_brand` varchar(200) DEFAULT NULL,
  `news_mall` varchar(200) DEFAULT NULL,
  `news_referrals` varchar(200) DEFAULT NULL COMMENT '爆料人',
  `news_direct_link` text,
  `news_reason` text COMMENT '原创区',
  `news_source` varchar(255) DEFAULT NULL COMMENT '消息来源',
  `news_update_author` bigint(20) DEFAULT NULL COMMENT '修改人id',
  `sync_main_id` bigint(20) DEFAULT '0' COMMENT '同步到主站以后的日志ID',
  `is_sync_main` tinyint(2) NOT NULL DEFAULT '0' COMMENT '是否同步到主站 ，默认为0，不同步，1，同步',
  `sina_id` bigint(20) NOT NULL DEFAULT '0',
  `tencent_id` bigint(20) NOT NULL DEFAULT '0',
  `news_pub_date` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  `worthy` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '值得数量',
  `unworthy` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '不值得数量',
  `associate_brand` tinyint(1) DEFAULT '0' COMMENT '是否关联品牌 0未关联，1关联',
  `expire` int(1) NOT NULL DEFAULT '0' COMMENT '是否过期：1.过期，默认为0',
  `post_mode` int(1) NOT NULL DEFAULT '0' COMMENT '日志展示模式：1，只显示标题',
  `is_top` int(1) NOT NULL DEFAULT '0' COMMENT '是否置顶：1.置顶',
  `associate_flag` tinyint(4) DEFAULT '0' COMMENT '是否已关联商家',
  `seo_title` text COMMENT 'seo标题',
  `seo_keywords` text COMMENT 'seo关键词',
  `seo_description` text COMMENT 'seo页面描述',
  `sum_collect_comment` int(11) DEFAULT '0' COMMENT '每次喜欢、每次评论都会增加1',
  `news_rzlx` varchar(50) NOT NULL DEFAULT 'xinpin',
  `is_write_post` tinyint(1) DEFAULT '0' COMMENT '立即同步主站标识',
  `set_auto_sync` tinyint(1) DEFAULT '0' COMMENT '自动同步主站标识',
  `is_write_post_time` datetime NOT NULL DEFAULT '0000-00-00 00:00:00' COMMENT '设置自动同步主站时间',
  `from_link` text,
  `from_name` varchar(100) DEFAULT NULL,
  `is_home_top` tinyint(1) DEFAULT '0' COMMENT '同步主站的数据是否置顶',
  `push_type` tinyint(1) DEFAULT '1' COMMENT '0不推送，1普通推送，2每日精选推送、3强制推送',
  `quote` longtext COMMENT '文章引用',
  `comment_status` varchar(20) NOT NULL DEFAULT 'open' COMMENT 'open 开启 closed 关闭',
  `news_direct_link_list` text COMMENT '其他直达链接',
  PRIMARY KEY (`id`),
  KEY `Index_status` (`news_status`,`news_pub_date`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=27330 DEFAULT CHARSET=utf8;



```

# 启动程序

```sh
# 启动程序
# test
python run.py --config=./config_test/pingce --func=pingce
python run.py --config=./config_test/shipin --func=shipin
python run.py --config=./config_test/wiki --func=wiki
python run.py --config=./config_test/xinrui --func=xinruipinpai
python run.py --config=./config_test/youhui --func=youhui
python run.py --config=./config_test/yuanchuang --func=yuanchuang
python run.py --config=./config_test/zixun --func=zixun

# online
python run.py --config=./config_online/pingce --func=pingce
python run.py --config=./config_online/shipin --func=shipin
python run.py --config=./config_online/wiki --func=wiki
python run.py --config=./config_online/xinrui --func=xinruipinpai
python run.py --config=./config_online/youhui --func=youhui
python run.py --config=./config_online/yuanchuang --func=yuanchuang
python run.py --config=./config_online/zixun --func=zixun
python run.py --config=./config_online/youhui --func=machine_report
python run.py --config=./config_online/home --func=home

python sync_tools.py --config=./config_test/pingce --func=pingce  --start_time='2017-01-01 00:00:00' --end_time='2017-05-25 00:00:00'
python sync_tools.py --config=./config_test/shipin --func=shipin  --start_time='2017-01-01 00:00:00' --end_time='2017-05-25 00:00:00'
python sync_tools.py --config=./config_test/xinrui --func=xinruipinpai  --start_time='2017-01-01 00:00:00' --end_time='2017-05-25 00:00:00'
python sync_tools.py --config=./config_test/youhui --func=youhui  --start_time='2017-01-01 00:00:00' --end_time='2017-05-25 00:00:00'
python sync_tools.py --config=./config_test/yuanchuang --func=yuanchuang  --start_time='2017-01-01 00:00:00' --end_time='2017-05-25 00:00:00'
python sync_tools.py --config=./config_test/zixun --func=zixun  --start_time='2017-01-01 00:00:00' --end_time='2017-05-25 00:00:00'

```

# 其它测试备份
```sh
# youhui
mysqldump -hyouhui_db_mysql_s04 -uyouhui_user -pxlJ7Enmhs -P3403 dbzdm_youhui youhui_tag_type_item -t --where='id in (2707811,2707777,2707771,2707725,2707723,2707713,2707709,2707707,2707687,2707685,2707661,2707623,2707617,2707607,2707587,2707567,2707561,2707555,2707549,2707521)' > youhui_tag_type_item.data


mysqldump -hyouhui_db_mysql_s04 -uyouhui_user -pxlJ7Enmhs -P3403 dbzdm_youhui youhui_category -t --where='id in (2707811,2707777,2707771,2707725,2707723,2707713,2707709,2707707,2707687,2707685,2707661,2707623,2707617,2707607,2707587,2707567,2707561,2707555,2707549,2707521)' > youhui_category.data

mysqldump -hyouhui_db_mysql_s04 -uyouhui_user -pxlJ7Enmhs -P3403 dbzdm_youhui youhui_extend -t --where='id in (2707811,2707777,2707771,2707725,2707723,2707713,2707709,2707707,2707687,2707685,2707661,2707623,2707617,2707607,2707587,2707567,2707561,2707555,2707549,2707521)' > youhui_extend.data

mysqldump -hyouhui_db_mysql_s04 -uyouhui_user -pxlJ7Enmhs -P3403 dbzdm_youhui youhui -t --where='id in (2707811,2707777,2707771,2707725,2707723,2707713,2707709,2707707,2707687,2707685,2707661,2707623,2707617,2707607,2707587,2707567,2707561,2707555,2707549,2707521)' > youhui.data



mysql -uroot -pxxx -P3306 xxx < youhui.data
mysql -uroot -pxxx -P3306 xxx < youhui_category.data
mysql -uroot -pxxx -P3306 xxx < youhui_extend.data
mysql -uroot -pxxx -P3306 xxx < youhui_tag_type_item.data


source /data/data/article_sync/youhui.data;
source /data/data/article_sync/youhui_category.data;
source /data/data/article_sync/youhui_extend.data;
source /data/data/article_sync/youhui_tag_type_item.data;

select id, sync_home,sync_home_time,is_home_top,updatetime from youhui_extend where id = 2707709;


select * from home_article where article_id= 2707709;
select * from youhui_tag_type_item where id = 2707709;
select * from youhui_category where id = 2707709;


update youhui_extend set updatetime='2017-05-08 19:07:00', sync_home_time='2017-05-08 19:07:00' , sync_home=1 where id = 2707709;

update youhui set brand_id = 100 where id = 2707709;
update youhui_tag_type_item set tag_id = 0 where article_id = 2707709;
update youhui_category set top_category_id=1, second_category_id=1, third_category_id=1, fourth_category_id=1 where youhui_id = 2707709;


# yuanchuang 

mysqldump -htest158_smzdm_yuanchuang -usmzdm_post -psmzdmPost_162304 smzdm_yuanchuang yuanchuang -t --where="id in (476222, 476221, 476220, 476219, 476218, 476217, 476216, 476215, 476214, 476213)" > yuanchuang.d
mysqldump -htest158_smzdm_yuanchuang -usmzdm_post -psmzdmPost_162304 smzdm_yuanchuang yuanchuang_extend -t --where="id in (476222, 476221, 476220, 476219, 476218, 476217, 476216, 476215, 476214, 476213)" > yuanchuang_extend.d
mysqldump -htest158_smzdm_yuanchuang -usmzdm_post -psmzdmPost_162304 smzdm_yuanchuang yuanchuang_brand_item -t --where="id in (107649,107646,107645,107644,107643,107642,107641,107640,107639,107638)" > yuanchuang_brand_item.d
mysqldump -htest158_smzdm_yuanchuang -usmzdm_post -psmzdmPost_162304 smzdm_yuanchuang yuanchuang_category_relation -t --where="id > 0 order by id desc limit 10" > yuanchuang_category_relation.d
mysqldump -htest158_smzdm_yuanchuang -usmzdm_post -psmzdmPost_162304 smzdm_yuanchuang yuanchuang_tag_item -t --where="id >0 order by id desc limit 10" > yuanchuang_tag_item.d

source /data/data/article_sync/yuanchuang_brand_item.d
source /data/data/article_sync/yuanchuang_category_relation.d
source /data/data/article_sync/yuanchuang.d
source /data/data/article_sync/yuanchuang_extend.d
source /data/data/article_sync/yuanchuang_tag_item.d


update yuanchuang set publishtime = '2017-05-09 15:45:00', is_delete=0, type=3;

update yuanchuang_tag_item set blog_id=476213 where id = 257268;
update yuanchuang_category_relation set yc_id=476213 where id = 395109;
update yuanchuang_brand_item set blog_id=476213 where id = 107649;

select * from yuanchuang_category_relation where yc_id=476213;
select * from yuanchuang_brand_item where blog_id=476213;
select * from yuanchuang_tag_item where blog_id=476213;
alter table yuanchuang_extend add column updateline datetime NOT NULL DEFAULT '1970-01-01 00:00:00' COMMENT '最后更新时间'

update yuanchuang_category_relation set level_first=100 where yc_id=476213;
update yuanchuang_brand_item set brand_id=100 where blog_id=476213;
update yuanchuang_tag_item set tag_id=100 where blog_id=476213;
update yuanchuang_extend set updateline = '2017-05-09 16:13:00' where id = 476213;

insert into yuanchuang_extend(id,content1,content2,remark,plid,district,have_read,add_modify,add_modify_time,seo_title,seo_keywords,seo_description,series_id,series_order_id,push_type,baidu_doc_id,probreport_id,is_write_post_time,set_auto_sync,is_home_top,from_vote,comment_switch,associate_brand,associate_mall,review_date,updateline) values (476213, '1','1','1','1','1','1','1','2017-05-09 15:57:00','1','1','1','1','1','1','1','1','2017-05-09 15:57:00','1','1','1','1','1','1','2017-05-09 15:57:00','2017-05-09 15:57:00')


# zixun

mysqldump -htest158_smzdm -usmzdm -psMzdmTest smzdm smzdm_news --where="id > 0 order by id desc limit 10"> smzdm_news.sql 
mysqldump -htest158_smzdm -usmzdm -psMzdmTest smzdm smzdm_brand_item --where="id > 0 order by id desc limit 10"> smzdm_brand_item.sql 
mysqldump -htest158_smzdm -usmzdm -psMzdmTest smzdm smzdm_tag_type_item --where="id > 0 order by id desc limit 10"> smzdm_tag_type_item.sql 

source /data/data/article_sync/smzdm_brand_item.sql
source /data/data/article_sync/smzdm_news.sql
source /data/data/article_sync/smzdm_tag_type_item.sql

update smzdm_news set news_pub_date='2017-05-09 17:53:00', news_status=1 ;
update smzdm_brand_item set blog_id=27320  where id = 845561;
update smzdm_tag_type_item set blog_id= 27320 where id = 6734613;

update smzdm_news set is_write_post_time = '2017-05-09 17:57:00' where id = 27320;
update smzdm_brand_item set brand_id=100  where id = 845561;
update smzdm_tag_type_item set tag_id= 100 where id = 6734613;

select brand_id from smzdm_brand_item where blog_id=27320

# pingce 
mysqldump -htest158_smzdm_probation -usmzdm -psMzdmTest -P3306 smzdm_probation  zdmdb_probreport --where="id>0 order by id desc limit 10" > zdmdb_probreport.sql
smzdm_tag_type_item


source /data/data/article_sync/zdmdb_probreport.sql

update zdmdb_probreport set publishtime= '2017-05-09 19:26:00', type = 3 , is_delete= 0;
update smzdm_tag_type_item set blog_id=29042 where id =6734602;

update zdmdb_probreport set is_write_post_time= '2017-05-09 19:33:00' where id = 29042;
update smzdm_tag_type_item set tag_id=100 where blog_id=29042;



# xinrui

mysqldump -htest158_newbrand -unewbrandUser -pe8dgHs3vmp -P3306 newbrandDB brand_special --where="id > 0 order by id desc limit 10" > brand_special.sql
mysqldump -htest158_newbrand -unewbrandUser -pe8dgHs3vmp -P3306 newbrandDB brand_special_item --where="id > 0 order by id desc limit 10" > brand_special_item.sql

source /data/data/article_sync/brand_special_item.sql
source /data/data/article_sync/brand_special.sql

update brand_special set pub_time='2017-05-10 13:50:00' ,status = 1;

update brand_special set editdate = '2017-05-10 13:58:00' where id = 100;
update 


# shipin
mysqldump -htest158_dbzdm_video -uvideo_user -pa8dfdEdsa03fdse9 -P3306 dbzdm_video v_video --where="id > 0 order by id desc limit 10" > v_video.sql
mysqldump -htest158_dbzdm_video -uvideo_user -pa8dfdEdsa03fdse9 -P3306 dbzdm_video v_brand_item --where="id > 0 order by id desc limit 10" > v_brand_item.sql
mysqldump -htest158_dbzdm_video -uvideo_user -pa8dfdEdsa03fdse9 -P3306 dbzdm_video v_tag_item --where="id > 0 order by id desc limit 10" > v_tag_item.sql
mysqldump -htest158_dbzdm_video -uvideo_user -pa8dfdEdsa03fdse9 -P3306 dbzdm_video v_video_extend --where="video_id > 0 order by video_id desc limit 10" > v_video_extend.sql


source /data/data/article_sync/v_brand_item.sql
source /data/data/article_sync/v_tag_item.sql
source /data/data/article_sync/v_video_extend.sql
source /data/data/article_sync/v_video.sql


update v_video set publish_date= '2017-05-10 11:36:00', status = 3, is_delete = 0;

select * from v_brand_item where video_id = 1007;
select * from v_tag_item where video_id = 1007;
select set_auto_sync,is_write_post_time,is_home_top from v_video_extend where video_id = 1007;

update v_video set modification_date='2017-05-10 11:47:00' where id = 1007;

```


# logstash-input-jdbc配置

## home_article.conf 配置

```sh
[root@10-9-84-225 conf]# cat home_article.conf 
input {
    stdin {
    }
    jdbc {
      # mysql jdbc connection string to our backup databse
      jdbc_connection_string => "jdbc:mysql://smzdm_recommend_mysql_m01:3306/recommendDB"
      # the user we wish to excute our statement as
      jdbc_user => "recommendUser"
      jdbc_password => "pVhXTntx9ZG"
      # the path to our downloaded jdbc driver
      jdbc_driver_library => "/opt/logstash/jars/mysql-connector-java-commercial-5.1.40-bin.jar"
      # the name of the driver class for mysql
      jdbc_driver_class => "com.mysql.jdbc.Driver"
      jdbc_paging_enabled => "true"
      jdbc_page_size => "50000"
      statement_filepath => "/opt/logstash/conf/home_article.sql"
      schedule => "* * * * *"
    }
}
filter {
    json {
        source => "message"
        remove_field => ["message"]
    }
}
output {
    elasticsearch {
        host => "10.9.84.225"
        port => "9200"
        protocol => "http"
        index => "home_article_index"
        document_type => "home_article"
        document_id => "%{id}"
      
        # cluster => "logstash-elasticsearch"
    }
    stdout {
        codec => json_lines
    }
}

```

## home_article.sql 配置

```sh
select id,
article_id,
channel,
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
from home_article
where auto_updatetime >= :sql_last_start

```

## home_article_index 结构

```sh

curl -XDELETE http://10.9.84.225:9200/home_article_index
curl -XPUT http://10.9.84.225:9200/home_article_index @home_article.json

PUT home_article_index/
{
  "mappings" : {
    "home_article" : {
      "dynamic" : "false",
      "properties" : {
        "id" : {
          "type" : "long",
          "store": "yes"
        },
        "article_id" : {
          "type" : "long",
          "store": "yes"
        },
        "channel_id" : {
          "type" : "integer",
          "store": "yes"
        },
        "channel" : {
          "type" : "string",
          "index" : "not_analyzed",
          "store": "yes"
        },
        "level1_ids" : {
          "type" : "string",
          "index": "not_analyzed",
          "store": "yes"
        },
 
       "level2_ids" : {
          "type" : "string",
          "index" : "not_analyzed",
          "store": "yes"
        },
        "level3_ids" : {
          "type" : "string",
          "index" : "not_analyzed",
          "store": "yes"
        },
        "level4_ids" : {
          "type" : "string",
          "index" : "not_analyzed",
          "store": "yes"
        },
        "tag_ids" : {
          "type" : "string",
          "index" : "not_analyzed",
          "store": "yes"
        },
        "brand_ids" : {
          "type" : "string",
          "index" : "not_analyzed",
          "store": "yes"
        },
        "sync_home" : {
          "type" : "integer",
          "store": "yes"
        },
        "is_top" : {
          "type" : "integer",
          "store": "yes"
        },
        "machine_report" : {
          "type" : "integer",
          "store": "yes"
        },
        "publish_time" : {
            "type" : "date",
            "store" : true,
            "format" : "yyyy-MM-dd HH:mm:ss"
  
      },
        "sync_home_time" : {
            "type" : "date",
            "store" : true,
            "format" : "yyyy-MM-dd HH:mm:ss"
        },
        "sync_time" : {
            "type" : "date",
            "store" : true,
            "format" : "yyyy-MM-dd HH:mm:ss"
        },
        "auto_updatetime" : {
            "type" : "date",
            "store" : true,
            "format" : "yyyy-MM-dd HH:mm:ss"
        },
        "status" : {
          "type" : "integer",
          "store": "yes"
     
        }
      }
    }
  }
}


```