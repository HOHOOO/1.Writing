import java.sql.{Connection, DriverManager, PreparedStatement}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by wangwei01 on 2017/3/24.
  * 用于统计详情页推荐的每个频道下每个算法的指标以及热门文章（pv 2000+）的详细推荐指标
  */
object article_details_statistic_day {

  def main(args: Array[String]): Unit = {

    val date = new Date()
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DATE, -1)
    val total_time = calendar.getTime()
    val formatter = new SimpleDateFormat("yyyyMMdd")
    //    val dateString = "20170326"
    var dateString = formatter.format(total_time)

    var tableNameStatistic = "t_day_article_details_recommend_statistic"
    var tableNameStatisticDetails = "t_day_article_details_recommend_statistic_details"
    //    var fields: String="time,s_channel_id,s_channel_name,source,modulename,r_article_displays,r_article_distinct_displays,r_article_clicks,s_article_clicks,cdr,ctr"
    var schema = "time:string,s_channel_id:int,s_channel_name:string,source:string,modulename:string,r_article_displays:int,r_article_distinct_displays:int,r_article_clicks:int,s_article_clicks:int,cdr:double,ctr:double"


    if (args.length >= 3){
      dateString = args(0)
      tableNameStatistic = args(1)
      tableNameStatisticDetails = args(2)
    }

    println("-------------------")
    println("dateString: %s".format(dateString))
    println("tableNameStatistic: %s".format(tableNameStatistic))
    println("tableNameStatisticDetails: %s".format(tableNameStatisticDetails))
    val conf = new SparkConf()

    val sc = new SparkContext(conf)

    val sqlContext = new HiveContext(sc)

    // 从用户浏览文章的nginx日志中提取所需字段
    // 用户浏览详情页的记录，即用户点击过每篇详情页的信息
    val sql_user_click_recommend_article_info =
    """
      |select regexp_extract(request, 'articles/(.*?)\\?', 1) as article_id,
      |regexp_extract(request, '/v2/(.*?)/articles', 1) as article_type,
      |if (regexp_extract(request, 'channel_id=(.*?)&', 1)='', '11', regexp_extract(request, 'channel_id=(.*?)&', 1)) as channel_id,
      |if (if (regexp_extract(request, 'channel_id=(.*?)&', 1)='', '11', regexp_extract(request, 'channel_id=(.*?)&', 1))='11', '11', '1') as channel_extend_id,
      |rs_id1, rs_id2, rs_id3, rs_id4, rs_id5,
      |request,
      |smzdm_client_name
      |from nginx_all_log
      |where ds=%s
    """.stripMargin.format(dateString)

    sqlContext.sql(sql_user_click_recommend_article_info).persist(StorageLevel.DISK_ONLY).registerTempTable("user_click_recommend_article_info")

    // 从当前详情页推荐回传的展示数据中提取所需字段
    // 详情页当前文章以及当前文章下展示的推荐文章信息
    val sql_details_current_and_recommend_article_info =
    """
      |select article_id as s_article_id,
      |channel_id as s_channel_id,
      |if (channel_id='11', '11', '1') as s_channel_extend_id,
      |submodulename as modulename,
      |r_article_id_or_article_name,
      |r_channel_id_or_price,
      |if (r_channel_id_or_price='11', '11', '1') as r_channel_extend_id,
      |if (rs_id4!='',
      |concat_ws('_', case source when '1' then '青岛' when '2' then '北京' else '百川' end, rs_id4),
      |case source when '1' then '青岛' when '2' then '北京' else '百川' end) as source,
      |rs_id1, rs_id2, rs_id3, rs_id4, rs_id5
      |from recommend.recommend_article_history
      |where ds=%s
      |and source in (1,2,3)
    """.stripMargin.format(dateString)

    sqlContext.sql(sql_details_current_and_recommend_article_info).persist(StorageLevel.DISK_ONLY).registerTempTable("details_current_and_recommend_article_info")

    // 构造用于统计不同频道、不同来源、不同模块的推荐文章点击量的表
    // 通过使用left join关联，is_click表示每个文章的详情页下的推荐文章是否被点击过
    val sql_details_recommend_article_click_info =
    """
      |select a.rs_id1 as trace_id,
      |a.s_article_id,
      |a.s_channel_id,
      |a.s_channel_extend_id,
      |a.r_article_id_or_article_name as r_article_id,
      |a.r_channel_id_or_price as r_channel_id,
      |a.r_channel_extend_id,
      |if(isnull(b.article_type), 0, 1) as is_click,
      |a.modulename,
      |a.source,
      |a.rs_id2,
      |a.rs_id3,
      |a.rs_id4
      |from details_current_and_recommend_article_info as a
      |left join user_click_recommend_article_info as b
      |on a.rs_id1 = b.rs_id1
      |and a.r_article_id_or_article_name = b.article_id
      |and a.r_channel_id_or_price = b.channel_id
      |and a.rs_id4 = b.rs_id4
      |and a.rs_id3 = b.rs_id3
      |and a.rs_id2 = b.rs_id2
    """.stripMargin
    sqlContext.sql(sql_details_recommend_article_click_info).registerTempTable("details_recommend_article_click_info")

    //--------------------------------------------------------//

    // 筛选出不同频道下的源文章(根据频道id+source+文章id组合去重)，用于之后统计源文章的点击量(pv)
    // 得到不同频道下去重后的文章id
    val sql_details_current_article_info =
    """
      |select
      |split(s_channel_id_and_article_id, '@')[0] as s_channel_id,
      |split(s_channel_id_and_article_id, '@')[1] as s_article_id,
      |s_channel_extend_id
      |from(
      |select distinct concat_ws('@', s_channel_id, s_article_id) as s_channel_id_and_article_id,
      |s_channel_extend_id
      |from details_current_and_recommend_article_info) as tmp
    """.stripMargin
    sqlContext.sql(sql_details_current_article_info).persist(StorageLevel.DISK_ONLY).registerTempTable("details_current_article_info")

    // 得到详情页展示过推荐文章的不同频道、不同来源、不同模块的源文章的点击信息
    // 有少部分is_click为0，说明 user_click_recommend_article_info 表中缺失了部分浏览文章详情页的记录
    val sql_details_current_article_click_info =
    """
      |select a.s_article_id,
      |a.s_channel_id,
      |a.s_channel_extend_id,
      |if(isnull(b.article_id), 0, 1) as is_click
      |from details_current_article_info as a
      |left join user_click_recommend_article_info as b
      |on a.s_article_id = b.article_id
      |and a.s_channel_id = b.channel_id
    """.stripMargin
    sqlContext.sql(sql_details_current_article_click_info).persist(StorageLevel.DISK_ONLY).registerTempTable("details_current_article_click_info")

    // 统计不同频道、不同来源、不同模块的所有文章的推荐文章点击量(pv)
    val sql_details_recommend_article_click_sta =
      """
        |select
        |s_channel_id,
        |source,
        |modulename,
        |count(r_article_id) as r_article_clicks
        |from details_recommend_article_click_info
        |where trace_id != ''
        |and is_click=1
        |group by s_channel_id, source, modulename
        |order by s_channel_id, source
      """.stripMargin

    // 统计不同频道、不同来源、不同模块的所有文章的推荐文章展示量
    val sql_details_recommend_article_display_sta =
      """
        |select
        |s_channel_id,
        |source,
        |modulename,
        |count(r_article_id_or_article_name) as r_article_displays,
        |count(distinct r_article_id_or_article_name) as r_article_distinct_displays
        |from details_current_and_recommend_article_info
        |group by s_channel_id, source, modulename
        |order by s_channel_id, source
      """.stripMargin

    // 统计不同频道下源文章的点击量(pv)
    val sql_details_current_article_click_sta =
      """
        |select s_channel_id, count(s_article_id) as s_article_clicks
        |from details_current_article_click_info
        |group by s_channel_id
      """.stripMargin


    sqlContext.sql(sql_details_recommend_article_display_sta).registerTempTable("details_recommend_article_display_sta")
    sqlContext.sql(sql_details_recommend_article_click_sta).registerTempTable("details_recommend_article_click_sta")
    sqlContext.sql(sql_details_current_article_click_sta).registerTempTable("details_current_article_click_sta")

    // 得到不同频道、不同来源、不同模块的所有文章的推荐文章展示量、点击量以及不同频道下有推荐文章展示的源文章的pv
    val sql_sum =
      """
        |select
        |%s as time,
        |a.s_channel_id,
        |case a.s_channel_id when 1 then '国内' when 2 then '发现' when 5 then '海淘' when 11 then '原创' end as s_channel_name,
        |a.source,
        |a.modulename,
        |a.r_article_displays,
        |a.r_article_distinct_displays,
        |b.r_article_clicks,
        |c.s_article_clicks,
        |round(b.r_article_clicks/a.r_article_displays, 4) as cdr,
        |round(b.r_article_clicks/c.s_article_clicks, 4) as ctr
        |from details_recommend_article_display_sta as a
        |join details_recommend_article_click_sta as b
        |on a.s_channel_id = b.s_channel_id
        |and a.source = b.source
        |and a.modulename = b.modulename
        |join details_current_article_click_sta as c
        |on a.s_channel_id = c.s_channel_id
      """.stripMargin.format(dateString)
    val article_details_recommend_statistic = sqlContext.sql(sql_sum)

    article_details_recommend_statistic.rdd.foreachPartition(iter => rdd2mysql(tableNameStatistic, schema, iter))


    //--------------------------------------------------------------------//
    // 统计有推荐文章展示的源文章中 点击量(pv)大于2000 的文章
    val sql_details_hot_current_article_click_sta =
    """
      |select tmp.s_channel_extend_id, tmp.s_channel_id, tmp.s_article_id, count(1) as s_article_clicks
      | from (
      |  select
      |  s_article_id,
      |  s_channel_id,
      |  s_channel_extend_id
      |  from details_current_article_click_info
      | ) as tmp
      |group by tmp.s_channel_extend_id, tmp.s_channel_id, tmp.s_article_id having count(1) > 2000
    """.stripMargin
    sqlContext.sql(sql_details_hot_current_article_click_sta).persist(StorageLevel.DISK_ONLY).registerTempTable("details_hot_current_article_click_sta")

    // 得到热门源文章（点击量（pv）大于2000）的的推荐文章展示信息
    val sql_details_hot_current_and_recommend_article_info =
      """
        |select
        |a.s_article_id,
        |a.s_channel_id,
        |a.s_channel_extend_id,
        |b.r_article_id_or_article_name,
        |b.r_channel_id_or_price,
        |b.r_channel_extend_id,
        |b.modulename,
        |b.source,
        |b.rs_id1,
        |b.rs_id2,
        |b.rs_id3,
        |b.rs_id4
        |from details_hot_current_article_click_sta as a
        |left join details_current_and_recommend_article_info as b
        |on a.s_article_id = b.s_article_id
        |and a.s_channel_id = b.s_channel_id
      """.stripMargin

    sqlContext.sql(sql_details_hot_current_and_recommend_article_info).persist(StorageLevel.DISK_ONLY).registerTempTable("details_hot_current_and_recommend_article_info")

    // 构造用于统计不同来源、不同频道的每个热门源文章的推荐文章点击量的表
    // 得到热门文章下展示过的推荐文章是被点击的信息
    val sql_details_hot_current_and_recommend_article_click_info =
    """
      |select
      |b.s_article_id,
      |b.s_channel_id,
      |b.s_channel_extend_id,
      |b.r_article_id,
      |b.r_channel_id,
      |b.r_channel_extend_id,
      |b.modulename,
      |b.source,
      |b.is_click
      |from details_hot_current_and_recommend_article_info as a
      |right join details_recommend_article_click_info as b
      |on a.s_article_id = b.s_article_id
      |and a.s_channel_id = b.s_channel_id
      |and a.rs_id1 = b.trace_id
      |and a.rs_id4 = b.rs_id4
      |and a.rs_id3 = b.rs_id3
      |and a.rs_id2 = b.rs_id2
      |where a.s_article_id != ''
    """.stripMargin

    sqlContext.sql(sql_details_hot_current_and_recommend_article_click_info).persist(StorageLevel.DISK_ONLY).registerTempTable("details_hot_current_and_recommend_article_click_info")

    // 统计不同来源、不同频道、不同热门文章的每个推荐文章的点击量
    val sql_details_hot_current_recommend_article_click_sta =
      """
        |select
        |source,
        |s_article_id,
        |s_channel_extend_id,
        |r_article_id,
        |r_channel_id,
        |sum(is_click) as r_article_clicks
        |from details_hot_current_and_recommend_article_click_info
        |group by source, s_channel_extend_id, s_article_id, r_article_id, r_channel_id
        |order by s_article_id desc, r_article_id desc
      """.stripMargin

    // 统计不同来源、不同频道、不同热门文章的每个推荐文章的展示量
    val sql_details_hot_current_recommend_article_display_sta =
      """
        |select
        |source,
        |s_article_id,
        |s_channel_extend_id,
        |r_article_id_or_article_name,
        |r_channel_id_or_price,
        |count(r_article_id_or_article_name) as r_article_displays
        |from details_hot_current_and_recommend_article_info
        |group by source, s_channel_extend_id, s_article_id, r_article_id_or_article_name, r_channel_id_or_price
        |order by s_article_id desc
      """.stripMargin

    sqlContext.sql(sql_details_hot_current_recommend_article_display_sta).registerTempTable("details_hot_current_recommend_article_display_sta")
    sqlContext.sql(sql_details_hot_current_recommend_article_click_sta).registerTempTable("details_hot_current_recommend_article_click_sta")

    // 得到不同来源、不同频道的不同热门文章的点击量、不同热门文章每篇的推荐文章的展示量以及该推荐文章的点击量
    val sql_hot_current_and_recommend_sta =
      """
        |select
        |a.source,
        |a.s_channel_extend_id,
        |c.s_channel_id,
        |a.s_article_id,
        |c.s_article_clicks,
        |a.r_article_displays,
        |b.r_article_id,
        |b.r_channel_id,
        |if(isnull(b.r_article_clicks), 0, b.r_article_clicks) as r_article_clicks,
        |round(b.r_article_clicks/a.r_article_displays, 4) as cdr
        |from details_hot_current_recommend_article_display_sta as a
        |left join details_hot_current_recommend_article_click_sta as b
        |on a.s_article_id = b.s_article_id
        |and a.s_channel_extend_id = b.s_channel_extend_id
        |and a.r_article_id_or_article_name = b.r_article_id
        |and a.r_channel_id_or_price = b.r_channel_id
        |and a.source = b.source
        |join details_hot_current_article_click_sta as c
        |on a.s_article_id = c.s_article_id
        |and a.s_channel_extend_id = c.s_channel_extend_id
        |order by a.s_article_id desc
      """.stripMargin

    sqlContext.sql(sql_hot_current_and_recommend_sta).persist(StorageLevel.DISK_ONLY).registerTempTable("hot_current_and_recommend_sta")

    // 得到热门文章中推荐文章的点展比的+top20和-top20的文章id
    val sql_hot_current_article_id_select_by_cdr_top20 =
      """
        |select s_article_id, s_channel_extend_id, round(sum(r_article_clicks)/sum(r_article_displays), 4) as sum_cdr
        |from hot_current_and_recommend_sta
        |group by s_article_id, s_channel_extend_id
        |order by sum_cdr desc limit 20
      """.stripMargin
    sqlContext.sql(sql_hot_current_article_id_select_by_cdr_top20).registerTempTable("top20")
    val sql_hot_current_article_id_select_by_cdr_top20_n =
      """
        |select s_article_id, s_channel_extend_id, round(sum(r_article_clicks)/sum(r_article_displays), 4) as sum_cdr
        |from hot_current_and_recommend_sta
        |group by s_article_id, s_channel_extend_id
        |order by sum_cdr limit 20
      """.stripMargin
    sqlContext.sql(sql_hot_current_article_id_select_by_cdr_top20_n).registerTempTable("top20_n")

    val sql_sql_hot_current_article_id_select_by_cdr_top40 =
      """
        |select s_article_id, s_channel_extend_id, sum_cdr from top20
        |union
        |select s_article_id, s_channel_extend_id, sum_cdr from top20_n
      """.stripMargin
    sqlContext.sql(sql_sql_hot_current_article_id_select_by_cdr_top40).registerTempTable("top40")

    val sql_filter_article_sta =
      """
        |select
        |a.source,
        |a.s_channel_extend_id,
        |a.s_channel_id,
        |a.s_article_id,
        |a.s_article_clicks,
        |a.r_article_displays,
        |a.r_article_id,
        |a.r_channel_id,
        |a.r_article_clicks,
        |a.cdr
        |from hot_current_and_recommend_sta as a
        |right join top40 as b
        |on a.s_article_id = b.s_article_id
        |and a.s_channel_extend_id = b.s_channel_extend_id
      """.stripMargin
    sqlContext.sql(sql_filter_article_sta).registerTempTable("hot_current_and_recommend_filter_sta")

    //    val prop = new java.util.Properties()
    //    prop.put("driver", "com.mysql.jdbc.Driver")
    //    sqlContext.read.jdbc("jdbc:mysql://smzdm_recommend_mysql_m01_184:3306/recommendDB?user=recommendUser&password=pVhXTntx9ZG&characterEncoding=utf8", "sync_youhui_primary", prop)
    //

    sqlContext.read.format("jdbc").options(
      Map("url" -> "jdbc:mysql://smzdm_recommend_mysql_m01_184:3306/recommendDB?zeroDateTimeBehavior=convertToNull",
        "driver" -> "com.mysql.jdbc.Driver",
        "dbtable" -> "sync_youhui_primary",
        "user" -> "recommendUser",
        "password" -> "pVhXTntx9ZG")).load().registerTempTable("sync_youhui_primary")

    sqlContext.read.format("jdbc").options(
      Map("url" -> "jdbc:mysql://yuanchuang_db_mysql:3306/smzdm_yuanchuang?zeroDateTimeBehavior=convertToNull",
        "driver" -> "com.mysql.jdbc.Driver",
        "dbtable" -> "yuanchuang",
        "user" -> "smzdm_post",
        "password" -> "smzdmPost_162304")).load().registerTempTable("yuanchuang")

    val sql_union_article_attribution =
      """
        |select id, title_prefix, title, sum_collect_comment from sync_youhui_primary
        |union
        |select id, NULL as title_prefix, title, sum_collect_comment from yuanchuang
      """.stripMargin
    sqlContext.sql(sql_union_article_attribution).registerTempTable("article")

    val sql_add_title =
      """
        |select
        |%s as time,
        |a2.source,
        |a2.s_channel_extend_id,
        |a2.s_channel_id,
        |a2.s_article_id,
        |a2.s_article_title_prefix,
        |a2.s_article_title,
        |a2.s_article_clicks,
        |a2.r_article_displays,
        |a2.r_article_id,
        |b2.title_prefix as r_article_title_prefix,
        |b2.title as r_article_title,
        |b2.sum_collect_comment,
        |a2.r_channel_id,
        |a2.r_article_clicks
        |from (
        |select
        |a.source,
        |a.s_channel_extend_id,
        |a.s_channel_id,
        |a.s_article_id,
        |b.title_prefix as s_article_title_prefix,
        |b.title as s_article_title,
        |a.s_article_clicks,
        |a.r_article_displays,
        |a.r_article_id,
        |a.r_channel_id,
        |a.r_article_clicks
        |from hot_current_and_recommend_filter_sta as a
        |left join article as b
        |on a.s_article_id = b.id
        |) as a2
        |left join article as b2
        |on a2.r_article_id = b2.id
      """.stripMargin.format(dateString)

    val prop = new java.util.Properties()
    prop.put("driver", "com.mysql.jdbc.Driver")
    prop.put("user", "wdDBUser")
    prop.put("password", "2Gb(tv+-n")
    sqlContext.sql(sql_add_title).write.mode(SaveMode.Append).jdbc("jdbc:mysql://dev_ga_data_warehouse_mysql_m01:3306/dev_ga_data_warehouse?user=wdDBUser&password=2Gb(tv+-n", tableNameStatisticDetails, prop)

  }

  //    article_details_recommend_statistic.rdd.foreachPartition(iter => rdd2mysql(tableName, schema, iter))
  //
  //    var values = article_details_recommend_statistic.rdd.collect.map{row =>
  //      val d = "'" + dateString + "'"
  //      val arr = row.toSeq.toArray
  //      arr(1) = "'" + arr(1) + "'"
  //      arr(2) = "'" + arr(2) + "'"
  //      arr(3) = "'" + arr(3) + "'"
  //      "(" + d + "," + arr.mkString(",") + ")"
  //    }.mkString(",")
  //
  //    to_mysql(tableName, fields, values)
  //  def to_mysql(tableName: String, fields: String, values: String): Unit ={
  //
  //    Class.forName("com.mysql.jdbc.Driver")
  //    val conn = DriverManager.getConnection("jdbc:mysql://dev_ga_data_warehouse_mysql_m01:3306/dev_ga_data_warehouse", "wdDBUser", "2Gb(tv+-n")
  //    // val conn = DriverManager.getConnection("jdbc:mysql://smzdm_recommend_mysql_m01_184:3306/recommendDB", "recommendUser", "pVhXTntx9ZG")
  //    val insert_sql =
  //      """
  //        |insert into %s (%s) values %s
  //      """.stripMargin.format(tableName, fields, values)
  //
  //    val ps = conn.prepareStatement(insert_sql)
  //    ps.executeUpdate()
  //
  //  }
  //
  //  val schema = "time:string,s_channel_id:int,s_channel_name:string,source:string,modulename:string,r_article_displays:int,r_article_distinct_displays:int,r_article_clicks:int,s_article_clicks:int,cdr:double,ctr:double"

  def rdd2mysql(tableName: String, schema: String, iterator: Iterator[org.apache.spark.sql.Row]): Unit ={

    def getFieldAndTypeTuple(schema: String): Array[(String, String)] = {
      schema.split(",").map{s =>
        val Array(field, fieldType) = s.trim.split(":")
        (field, fieldType)
      }
    }

    val iterArray = iterator.toArray
    val fieldAndTypeTuple: Array[(String, String)] = getFieldAndTypeTuple(schema)
    val fields = fieldAndTypeTuple.map(_._1).mkString(",")
    val values = (for (i <- 0 until fieldAndTypeTuple.length) yield "?").mkString(",")

    var conn: Connection = null
    var ps: PreparedStatement = null
    val sql = "insert into %s (%s) values (%s)".format(tableName, fields, values)
    try {
      conn = DriverManager.getConnection("jdbc:mysql://dev_ga_data_warehouse_mysql_m01:3306/dev_ga_data_warehouse", "wdDBUser", "2Gb(tv+-n")
      conn.setAutoCommit(false)
      ps = conn.prepareStatement(sql)
      // row即为每个rdd中的row
      // [20170327,1,国内,青岛,相关优惠,3890260,2363,10603,1664949,0.0027,0.0064]
      for (row <- iterArray){
        ps.clearBatch()
        for (i <- 0 until row.length){
          val fieldType = fieldAndTypeTuple(i)._2
          if ( fieldType == "string"){
            ps.setString(i + 1, row(i).toString)
          }else if (fieldType == "int"){
            ps.setInt(i + 1, row(i).toString.toInt)
          }else if (fieldType == "double"){
            ps.setDouble(i + 1, row(i).toString.toDouble)
          }

        }
        ps.addBatch()
        ps.executeBatch()
      }

      conn.commit()

    } catch {
      case e: Exception => println(e)
    } finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    }

  }


}
