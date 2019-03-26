package cn.com.snsoft.user_tags

import java.io.File

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
  * 用户行为标签，偏好+权重，合并版
  *
  * 处理第二天的数据，与前一天的数据合并
  * 具体步骤：
  * 从历史标签中取出用户的行为标签（type_id,weight）,存在HBASE表中的be_tags列族中
  * 用户产品标签（prod_id,weight），存在HBASE的prod_tags列簇中
  *
  *
  * 处理数据得到的新的标签要与之前的标签做对比，如果type_id存在，则weight相加,并乘以衰减系数
  * 若不存在，则乘以衰减系数后再存到be_tags列族中
  *
  * prod_id同上
  *
  *
  */
object mkBeTagsPlus {
  def main(args: Array[String]): Unit = {
    //todo 参数控制

    if (args.length != 1) {
      println(
        """
          |be_tags
          |参数：分区字段
        """.stripMargin)
      sys.exit()
    }


    //todo 指定hive的源数据库
    val warehouseLocation = new File("jdbc:mysql://114.247.63.163:3306/hive").getAbsolutePath

    //todo 创建sparksession

    val spark: SparkSession = SparkSession.builder()
      .appName("mkBeTagsPlus")
      //.master("spark://114.247.63.163:7337")
      //.master("local[1]")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport() //TODO 开启支持hive
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") //todo 采用KRYO序列化
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN") //设置日志输出级别


    //todo 指定数据库
    spark.sql("use snsoft_blc")

    //todo 操作fdm_blc_member表,按分区查询
    val behavior_DF = spark.sql(
      s"""
select * from fdm_blc_behavior where dt = ${args(0)}
      """.stripMargin)

    //todo 指定衰减系数
    val coefficient = 0.75

    //todo 定义一个阈值，如果某个标签的权重小于这个阈值，则删除这个标签
    val threshold = 0.5
    import scala.collection.immutable.Map

    //todo 得到的（member_id,Map(type_id,权重)，Mao(prod_id,权重)）
    val idAndTypeAndProdRdd: RDD[(String, (Map[String, Double], Map[String, Double]))] = behavior_DF.rdd.mapPartitions(itea => {
      itea.map(row => {
        val id = row.getAs[String]("id")
        var type_id = Map[String, Double]()
        var prod_id = Map[String, Double]()
        type_id = be_tagsPlus.mkTags(row).filter(!_._1.startsWith("prod_"))
        prod_id = be_tagsPlus.mkTags(row).filter(_._1.startsWith("prod_"))
        (id, (type_id, prod_id))

      })
    })


    // todo 根据用户ID聚合，map中的相同k的v相加
    //todo ( map1 /: map2 ) { case (map, (k,v)) => map + ( k -> (v + map.getOrElse(k, 0)) ) }
    val reduceRdd: RDD[(String, (Map[String, Double], Map[String, Double]))] = idAndTypeAndProdRdd.reduceByKey({
      case (a, b) =>
        val type_tags = (a._1 /: b._1) ((map, kv) => {
          map + (kv._1 -> (kv._2 + map.getOrElse(kv._1, 0.0)))
        })

        val prod_tags = (a._2 /: b._2) ((map, kv) => {
          map + (kv._1 -> (kv._2 + map.getOrElse(kv._1, 0.0)))
        })
        (type_tags, prod_tags)
    })


    // res_rdd.collect().foreach(println)

    val hbaseTableName = "snsoft_blc:blc_user"

    reduceRdd.foreachPartition(itea => {

      //todo 建立HBASE连接

      //todo 建立HBASE连接
      val hconf = HBaseConfiguration.create()
      hconf.set("hbase.zookeeper.quorum", "cdh1,cdh2,cdh3")
      hconf.set("hbase.zookeeper.property.clientPort", "2181")
      hconf.set("hbase.defaults.for.version.skip", "true")

      //TODO 在每个分区中建立连接
      val connection: Connection = ConnectionFactory.createConnection(hconf)
      val tableName: TableName = TableName.valueOf(hbaseTableName)

      val table: Table = connection.getTable(tableName)

      itea.foreach(f = x => {
        //todo 用户ID
        val id = x._1

        val get1 = new Get(Bytes.toBytes(id))
        val get2 = new Get(Bytes.toBytes(id))

        val put = new Put(Bytes.toBytes(id))

        //TODO 得到be_tags中的数据
        val be_tagsGet: Get = get1.addFamily(Bytes.toBytes("be_tags"))

        val prod_tagsGet: Get = get2.addFamily(Bytes.toBytes("prod_tags"))

        val be_tagsResult = table.get(be_tagsGet)
        val prod_tagsResult = table.get(prod_tagsGet)

        val map1: mutable.Map[String, Double] = mutable.Map[String, Double]()
        val map2: mutable.Map[String, Double] = mutable.Map[String, Double]()

        //todo 定义一个Boolean值，如果be_tags中有值，则为true，如果没有，则为false
        var boolean = true

        //todo 导入Java集合转为为scala的隐式转化，遍历上面查询出的结果集，并放到一个map集合中
        import scala.collection.JavaConversions._
        try {

          for {
            cell <- be_tagsResult.listCells()
          } {
            val type_id = Bytes.toString(CellUtil.cloneQualifier(cell))
            val weight: Double = Bytes.toString(CellUtil.cloneValue(cell)).toDouble
            //todo 将得到的（type_id,weight）添加到map中
            map1 += type_id -> weight
          }

          for (cell <- prod_tagsResult.listCells()) {
            val prod_id = Bytes.toString(CellUtil.cloneQualifier(cell))
            val weight = Bytes.toString(CellUtil.cloneValue(cell)).toDouble

            map2 += prod_id -> weight

          }
        } catch {
          case _: NullPointerException => boolean = false
        }

        //todo 如果为true，则说明该用户是历史用户
        if (boolean) {
          //todo 历史数据
          val keyset1: collection.Set[String] = map1.keySet
          val keyset2: collection.Set[String] = map2.keySet

          //TODO 新的数据
          val set1: Predef.Set[String] = x._2._1.keySet
          val set2: Predef.Set[String] = x._2._2.keySet

          //todo 求俩个set集合的交集，求出历史标签与新增标签的并集
          val intersectSet1: collection.Set[String] = keyset1.intersect(set1)
          val intersectSet2: collection.Set[String] = keyset2.intersect(set2)

          val map3: mutable.Map[String, Double] = mutable.Map[String, Double]()

          //todo 判断type_tags中新的标签与历史标签是否有交集

          if (intersectSet1.nonEmpty) {
            //todo 相同的type_id权重相加，乘以衰减系数
            for (i <- intersectSet1) {
              val old_weight: Double = map1.getOrElse(i, 0)
              val new_weight: Double = x._2._1.getOrElse(i, 0.0)
              map3 += i -> (old_weight + new_weight) * coefficient
            }


            //todo 历史标签中有，新的标签中没有的，权重乘以衰减系数，再存入HBASE
            for (j <- keyset1.diff(set1)) {
              val old_weight: Double = map1.getOrElse(j, 0)
              map3 += j -> (old_weight * coefficient)
            }
            //todo 新标签中有，历史标签中没有的

            for (j <- set1.diff(keyset1)) {
              val new_weight: Double = x._2._1.getOrElse(j, 0)
              map3 += j -> (new_weight * coefficient)
            }

          } else {
            //todo 如果没有交集
            for (i <- keyset1) {
              val old_weight: Double = map1.getOrElse(i, 0.0)
              map3 += i -> (old_weight * coefficient)
            }

            for (i <- set1) {
              val new_weight: Double = x._2._1.getOrElse(i, 0.0)
              map3 += i -> (new_weight * coefficient)
            }
          }


          val map4: mutable.Map[String, Double] = mutable.Map[String, Double]()


          //todo 判断prod_tags中新的标签和历史标签是否有交集
          if (intersectSet2.nonEmpty) {
            //todo 相同的type_id权重相加，乘以衰减系数
            for (i <- intersectSet2) {
              val old_weight: Double = map2.getOrElse(i, 0)
              val new_weight: Double = x._2._2.getOrElse(i, 0.0)
              map4 += i -> (old_weight + new_weight) * coefficient
            }

            //todo 历史标签中有，新的标签中没有的，权重乘以衰减系数，再存入HBASE
            for (j <- keyset2.diff(set2)) {
              val old_weight: Double = map2.getOrElse(j, 0)
              map4 += j -> (old_weight * coefficient)
            }
            //todo 新标签中有，历史标签中没有的

            for (j <- set2.diff(keyset2)) {
              val old_weight: Double = x._2._2.getOrElse(j, 0)
              map4 += j -> (old_weight * coefficient)
            }

          } else {
            //todo 如果没有交集
            for (i <- keyset2) {
              val old_weight = map2.getOrElse(i, 0.0)
              map4 += i -> (old_weight * coefficient)
            }

            for (i <- set2) {
              val new_weight: Double = x._2._2.getOrElse(i, 0.0)
              map4 += i -> (new_weight * coefficient)
            }
          }


          map3.filter(_._2 > threshold).foreach(p => {
            put.addColumn(Bytes.toBytes("be_tags"), Bytes.toBytes(p._1), Bytes.toBytes(p._2.formatted("%.2f")))
          })

          map4.filter(_._2 > threshold).foreach(p => {
            put.addColumn(Bytes.toBytes("prod_tags"), Bytes.toBytes(p._1), Bytes.toBytes(p._2.formatted("%.2f")))
          })


        } else {
          //todo 如果get不到数据，则说明此用户是新用户
          x._2._1.foreach(i => {
            put.addColumn(Bytes.toBytes("be_tags"), Bytes.toBytes(i._1), Bytes.toBytes((i._2 * coefficient).formatted("%.2f")))
          })

          x._2._2.foreach(i => {
            put.addColumn(Bytes.toBytes("prod_tags"), Bytes.toBytes(i._1), Bytes.toBytes((i._2 * coefficient).formatted("%.2f")))

          })

        }
        table.put(put)

      })
      //todo 关闭连接
      table.close()
      connection.close()

    })


    //todo 资源关闭
    spark.stop()

  }
}


