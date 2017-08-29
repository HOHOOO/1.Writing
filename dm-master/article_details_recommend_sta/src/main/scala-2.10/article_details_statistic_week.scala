import java.sql.{Connection, DriverManager, PreparedStatement}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by wangwei01 on 2017/3/24.
  * 用于统计详情页推荐的不同算法类型下不同日志类型的推荐情况
  */
object article_details_statistic_week {

  def main(args: Array[String]): Unit = {

    val date = new Date()
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DATE, -1)
    val total_time = calendar.getTime()
    val formatter = new SimpleDateFormat("yyyyMMdd")
    //    val dateString = "20170326"
    var dateString = formatter.format(total_time)

    var tableNameStatistic = "t_week_article_details_recommend_statistic"
    //    var fields: String="time,s_channel_id,s_channel_name,source,modulename,r_article_displays,r_article_distinct_displays,r_article_clicks,s_article_clicks,cdr,ctr"
    var schema = "time:string,source:string,s_yh_type:string,modulename:string,r_article_displays:int,r_article_distinct_displays:int,r_article_clicks:int,s_article_clicks:int,cdr:double,ctr:double"


    if (args.length >= 2){
      dateString = args(0)
      tableNameStatistic = args(1)
    }

    println("-------------------")
    println("dateString: %s".format(dateString))
    println("tableNameStatistic: %s".format(tableNameStatistic))
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

    sqlContext.read.format("jdbc").options(
      Map("url" -> "jdbc:mysql://smzdm_recommend_mysql_m01_184:3306/recommendDB?zeroDateTimeBehavior=convertToNull",
        "driver" -> "com.mysql.jdbc.Driver",
        "dbtable" -> "sync_youhui_primary",
        "user" -> "recommendUser",
        "password" -> "pVhXTntx9ZG")).load().registerTempTable("sync_youhui_primary")

    // 从当前详情页推荐回传的展示数据中提取所需字段
    // 详情页当前文章以及当前文章下展示的推荐文章信息
    val sql_details_current_and_recommend_article_info =
    """
      |select
      |a.s_article_id,
      |a.s_channel_id,
      |a.s_channel_extend_id,
      |a.modulename,
      |a.r_article_id_or_article_name,
      |a.r_channel_id_or_price,
      |a.r_channel_extend_id,
      |a.source,
      |a.rs_id1, a.rs_id2, a.rs_id3, a.rs_id4, a.rs_id5,
      |b.yh_type as s_yh_type
      |from (
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
      |and source in (1,2,3)) as a
      |left join sync_youhui_primary as b
      |on a.s_article_id = b.id
      |where a.s_channel_extend_id = 1
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
      |a.s_yh_type,
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
    // 得到不同频道、不同模块、不同来源去重后的文章id
    val sql_details_current_article_info =
    """
      |select
      |split(key, '@')[0] as s_channel_id,
      |split(key, '@')[1] as s_article_id,
      |split(key, '@')[2] as modulename,
      |split(key, '@')[3] as source,
      |split(key, '@')[4] as s_yh_type,
      |s_channel_extend_id
      |from(
      |select distinct concat_ws('@', s_channel_id, s_article_id, modulename, source, s_yh_type) as key,
      |s_channel_extend_id
      |from details_current_and_recommend_article_info) as tmp
    """.stripMargin
    sqlContext.sql(sql_details_current_article_info).registerTempTable("details_current_article_info")

    // 得到详情页不同频道、不同来源、不同模块的源文章的点击信息
    val sql_details_current_article_click_info =
      """
        |select a.s_article_id,
        |a.s_channel_id,
        |a.s_channel_extend_id,
        |if(isnull(b.article_id), 0, 1) as is_click,
        |a.s_yh_type,
        |a.source,
        |a.modulename
        |from details_current_article_info as a
        |left join user_click_recommend_article_info as b
        |on a.s_article_id = b.article_id
        |and a.s_channel_id = b.channel_id
      """.stripMargin
    sqlContext.sql(sql_details_current_article_click_info).registerTempTable("details_current_article_click_info")

    // 统计不同来源、不同文章类型、不同模块的所有文章的推荐文章点击量(pv)
    val sql_details_recommend_article_click_sta =
      """
        |select
        |source,
        |s_yh_type,
        |modulename,
        |sum(is_click) as r_article_clicks
        |from details_recommend_article_click_info
        |where trace_id != ''
        |group by source, s_yh_type, modulename
        |order by source, s_yh_type, modulename
      """.stripMargin

    // 统计不同来源、不同文章类型、不同模块的所有文章的推荐文章展示量
    val sql_details_recommend_article_display_sta =
      """
        |select
        |source,
        |s_yh_type,
        |modulename,
        |count(r_article_id_or_article_name) as r_article_displays,
        |count(distinct r_article_id_or_article_name) as r_article_distinct_displays
        |from details_current_and_recommend_article_info
        |group by source, s_yh_type, modulename
        |order by source, s_yh_type, modulename
      """.stripMargin

    // 统计不同来源、不同文章类型、不同模块的所有源文章的点击量(pv)
    val sql_details_current_article_click_sta =
      """
        |select source, s_yh_type, modulename, count(s_article_id) as s_article_clicks
        |from details_current_article_click_info
        |group by source, s_yh_type, modulename
        |order by source, s_yh_type, modulename
      """.stripMargin

    sqlContext.sql(sql_details_recommend_article_display_sta).registerTempTable("details_recommend_article_display_sta")
    sqlContext.sql(sql_details_recommend_article_click_sta).registerTempTable("details_recommend_article_click_sta")
    sqlContext.sql(sql_details_current_article_click_sta).registerTempTable("details_current_article_click_sta")

    val sql_sum =
      """
        |select
        |%s as time,
        |a.source,
        |a.s_yh_type,
        |a.modulename,
        |a.r_article_displays,
        |a.r_article_distinct_displays,
        |b.r_article_clicks,
        |c.s_article_clicks,
        |round(b.r_article_clicks/a.r_article_displays, 4) as cdr,
        |round(b.r_article_clicks/c.s_article_clicks, 4) as ctr
        |from details_recommend_article_display_sta as a
        |join details_recommend_article_click_sta as b
        |on a.source = b.source
        |and a.s_yh_type = b.s_yh_type
        |and a.modulename = b.modulename
        |join details_current_article_click_sta as c
        |on a.source = c.source
        |and a.s_yh_type = c.s_yh_type
        |and a.modulename = c.modulename
        |order by a.source, a.s_yh_type, a.modulename
      """.stripMargin.format(dateString)
    val article_details_recommend_statistic = sqlContext.sql(sql_sum)

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
    article_details_recommend_statistic.rdd.foreachPartition(iter => rdd2mysql(tableNameStatistic, schema, iter))
  }

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
