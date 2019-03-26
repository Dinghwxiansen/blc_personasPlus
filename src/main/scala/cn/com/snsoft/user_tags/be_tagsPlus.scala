package cn.com.snsoft.user_tags

import org.apache.spark.sql.Row

/**
  * 用户行为标签 具体到用户浏览过的产品
  */
object be_tagsPlus extends behaviour_tags {
  override def mkTags(args: Any*): Map[String, Double] = {
    import scala.collection.immutable.Map
    var map = Map[String, Double]()

    //todo 参数解析
    val row = args(0).asInstanceOf[Row]
    var type_id: String = ""


    //todo type_id

    type_id = row.getAs[String]("type_id")

    if (!type_id.isEmpty) {
      val behavior_id = row.getAs[String]("behavior_id")
      var weight = 0

      //TODO 对每种行为类型做模式匹配，匹配不同的权重
      behavior_id match {
        case "0" => weight = 8
        case "1" => weight = 7
        case "2" => weight = 6
        case "3" => weight = 5
        case "4" => weight = 4
        case "5" => weight = 3
        case "6" => weight = 2
        case "7" => weight = 1
        case _ => weight = 0
      }

      map += type_id -> weight

    } else {
      map += "type_id" -> 0.00
    }


    //todo  prod_id
    var prod_id: String = ""
    try {
      prod_id = row.getAs[String]("prod_id")

    } catch {
      case _: NullPointerException => map += "prod_id" -> 0.01
    }


    //todo 声明一个可变的变量，表示每次行为的权重


    // todo 解析用户的行为类型
    val behavior_id = row.getAs[String]("behavior_id")
    var weight = 0

    //TODO 对每种行为类型做模式匹配，匹配不同的权重
    behavior_id match {
      case "0" => weight = 8
      case "1" => weight = 7
      case "2" => weight = 6
      case "3" => weight = 5
      case "4" => weight = 4
      case "5" => weight = 3
      case "6" => weight = 2
      case "7" => weight = 1
      case _ => weight = 0
    }


    map += "prod_" + prod_id -> weight


    map
  }
}
