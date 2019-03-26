package cn.com.snsoft.user_tags

import cn.com.snsoft.utils.getAge
import org.apache.spark.sql.Row


/**
  * 用户基本属性标签
  */
object basic_tags extends tags {
  override def mkTags(args: Any*): Map[String, String] = {
    import scala.collection.immutable.Map


    var map = Map[String, String]()

    //TODO 参数解析
    val row = args(0).asInstanceOf[Row]

    //todo 1 获取gender并匹配

    try {
      val gender = row.getAs[Int]("gender")
      gender match {
        case 0 => map += "gender" -> "001001"
        case 1 => map += "gender" -> "001002"
      }
    } catch {
      case _: NullPointerException => map += "gender" -> "unknown"
    }

    //TODO  2 获取age并匹配

    try {
      val birth = row.getAs[String]("birth")
      //todo 根据生日计算年龄
      val age = getAge.getAgeByBirthDay(birth)

      age match {
        case i if i < 15 => map += "age" -> "002001"
        case i if i >= 15 && i <= 27 => map += "age" -> "002002"
        case i if i >= 28 && i <= 34 => map += "age" -> "002003"
        case i if i >= 35 && i <= 44 => map += "age" -> "002004"
        case i if i >= 45 && i <= 60 => map += "age" -> "002005"
        case i if i > 60 => map += "age" -> "002006"
        case _ => map += "age" -> "unknown"
      }

    } catch {
      case _: NullPointerException => map += "age" -> "unknown"
    }


    //todo 3 deformity_level 伤残等级

    try {
      val deformity_level = row.getAs[Int]("deformity_level")
      deformity_level match {
        case 1 => map += "deformity_level" -> "009001"
        case 2 => map += "deformity_level" -> "009002"
        case 3 => map += "deformity_level" -> "009003"
        case 4 => map += "deformity_level" -> "009004"
        case _ => map += "deformity_level" -> "unknown"

      }

    } catch {
      case _: NullPointerException => map += "deformity_level" -> "unknown"
    }


    //todo 4 education_level  教育等级

    try {
      val education_level = row.getAs[Int]("education_level")
      education_level match {
        case 1 => map += "education_level" -> "003001"
        case 2 => map += "education_level" -> "003002"
        case 3 => map += "education_level" -> "003003"
        case 4 => map += "education_level" -> "003004"
        case 5 => map += "education_level" -> "003005"
        case 6 => map += "education_level" -> "003006"
        case 7 => map += "education_level" -> "003007"
        case 8 => map += "education_level" -> "003008"
        case 9 => map += "education_level" -> "003009"
        case _ => map += "education_level" -> "unknown"
      }
    } catch {
      case _: NullPointerException => map += "education_level" -> "unknown"
    }

    //TODO 5 position 职业不需要做模式匹配，直接输出code

    try {
      val position = row.getAs[String]("position")
      if (position.isEmpty) {
        map += "position" -> "unknown"
      } else {
        map += "position" -> position.toString
      }

    } catch {
      case _: NullPointerException => map += "position" -> "unknown"
    }


    //todo 6 hobby兴趣好爱，直接输出
    try {
      val hobby = row.getAs[String]("hobby")
      if (hobby.isEmpty) {
        map += "hobby" -> "unknown"
      } else {
        map += "hobby" -> hobby
      }

    } catch {
      case _: NullPointerException => map += "hobby" -> "unknown"
    }


    //TODO 7 terminal_type 用户注册终端类型
    try {
      val terminal_type = row.getAs[Int]("terminal_type")

      terminal_type match {
        case 0 => map += "terminal_type" -> "005001"
        case 1 => map += "terminal_type" -> "005002"
        case 2 => map += "terminal_type" -> "005003"
        case 3 => map += "terminal_type" -> "005004"
        case _ => map += "terminal_type" -> "unknown"
      }
    } catch {
      case _: NullPointerException => map += "terminal_type" -> "unknown"
    }


    // todo 获取省份

    try {
      val province = row.getAs[String]("province")
      if (province.isEmpty) {
        map += "province" -> "unknown"
      } else {
        map += "province" -> province
      }

    } catch {
      case _: NullPointerException => map += "province" -> "unknown"

    }


    //TODO 所在城市
    try {
      val city = row.getAs[String]("city")
      if (city.isEmpty) {
        map += "city" -> "unknown"
      } else {
        map += "city" -> city
      }

    } catch {
      case _: NullPointerException => map += "city" -> "unknown"

    }


    //todo 城市等级

    try {
      val level = row.getAs[String]("city_level")
      level match {
        case "一线城市" => map += "city_level" -> "007001"
        case "二线城市" => map += "city_level" -> "007002"
        case "三线城市" => map += "city_level" -> "007003"
        case _ => map += "city_level" -> "unknown"
      }
    } catch {

      case _: NullPointerException => map += "city_level" -> "unknown"
    }


    map

  }

}
