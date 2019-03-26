package cn.com.snsoft.user_tags

import org.apache.spark.sql.Row

/**
  * 用户浏览记录表可得到的行为标签（type_id,上午，下午。。。。。）
  */
object browse_Tags extends behaviour_tags {

  override def mkTags(args: Any*): Map[String, Double] = {
    import scala.collection.immutable.Map
    var map: Map[String, Double] = Map[String, Double]()

    //todo 参数解析
    val row = args(0).asInstanceOf[Row]
    try {
      //todo 用户每次浏览的type_id
//      val type_id = row.getAs[String]("type_id")

      //todo 解析参数得到
      val start_date = row.getAs[String]("browse_time")

      //todo 2018-07-29 06:14:23.0,解析
      val time = start_date.substring(11, 13).toLong

      time match {
        case i if i >= 5 && i < 8 => map += "015001" -> 1
        case i if i >= 8 && i < 11 => map += "015002" -> 1
        case i if i >= 11 && i < 13 => map += "015003" -> 1
        case i if i >= 13 && i < 18 => map += "015004" -> 1
        case i if i >= 18 && i < 24 => map += "015005" -> 1
        case i if i >= 0 && i < 5 => map += "015005" -> 1
      }

    } catch {
      case _: NullPointerException => map += "time_tags" -> 0.00
    }

    map
  }
}
