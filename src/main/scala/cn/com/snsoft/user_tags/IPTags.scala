package cn.com.snsoft.user_tags

import org.apache.spark.sql.Row

/**
  * 解析用户的ip地址，得到用户所在省份,以及所在城市等级标签
  */
object IPTags extends tags {
  override def mkTags(args: Any*): Map[String, String] = {
    import scala.collection.immutable.Map
    var map = Map[String, String]()

    //todo 参数解析
    val row = args(0).asInstanceOf[Row]

    //todo ip地址解析
    //val ip = row.getAs[String]("ip")

    // todo 调用Java中的方法得到用户的省份，城市(pass)
    //val areaMap: util.Map[String, String] = getIpArea.getIpArea(ip)

    //map += "province" -> areaMap.get("region")
    //map += "city" -> areaMap.get("city")
    // map += "isp" -> areaMap.get("isp")

    // todo 获取省份
    val province = row.getAs[String]("province")
    map += "province" -> province

    //TODO 所在城市
    val city = row.getAs[String]("city")
    map += "city" -> city

    //todo 城市等级
    val level = row.getAs[String]("city_level")
    level match {
      case "一线城市" => map += "city_level" -> "007001"
      case "二线城市" => map += "city_level" -> "007002"
      case "三线城市" => map += "city_level" -> "007003"
      case _ => map += "city_level" -> "未知"
    }

    map
  }
}
