package cn.com.snsoft.user_tags

import java.io.File

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 用户浏览记录表处理,time_tags处理
  *cn.com.snsoft.user_tags.mkBrowseTags
  */
object mkBrowseTags {
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
      .appName("mkBrowseTags")
      //.master("spark://114.247.63.163:7337")
      //.master("local[1]")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport() //TODO 开启支持hive
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") //todo 采用KRYO序列化
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN") //设置日志输出级别


    //todo 指定要查询的数据库
    spark.sql("use snsoft_blc")

    //todo 操作fdm_blc_prod_browse   用户浏览记录表
    val browseDF: DataFrame = spark.sql(
      s"""
select * from
         fdm_blc_prod_browse where dt = ${args(0)}

      """.stripMargin)


    val browse_Rdd: RDD[(String, Map[String, Double])] = browseDF.rdd.mapPartitions(itea => {
      itea.map(row => {
        val id = row.getAs[String]("id")

        val tags: Map[String, Double] = browse_Tags.mkTags(row)

        (id, tags)

      })
    })

    //todo 根据ID聚合操作，把相同K的map相加,得到的结果为（用户标识，（时间段，浏览次数））
    val reduce_Rdd: RDD[(String, Map[String, Double])] = browse_Rdd.reduceByKey({
      case (a, b) =>
        (a /: b) {
          case (map, (k, v)) => map + (k -> (v + map.getOrElse(k, 0.0)))
        }
    })


    //sort_rdd.foreach(println)

    //todo 指定HBASE操作的表
    val hbaseTableName = "snsoft_blc:blc_user"

    reduce_Rdd.foreachPartition(itea => {
      //todo 建立HBASE连接
      val hconf = HBaseConfiguration.create()
      hconf.set("hbase.zookeeper.quorum", "cdh1,cdh2,cdh3")
      hconf.set("hbase.zookeeper.property.clientPort", "2181")
      hconf.set("hbase.defaults.for.version.skip", "true")

      //TODO 在每个分区中建立连接
      val connection: Connection = ConnectionFactory.createConnection(hconf)
      val tableName: TableName = TableName.valueOf(hbaseTableName)

      val table: Table = connection.getTable(tableName)

      itea.foreach(i => {

        //todo 用户ID
        val id = i._1

        //todo 用户行为标签的合并
        // todo 跟据用户ID去查
        val get = new Get(Bytes.toBytes(id))

        //todo put写入HBASE，并指定每行的rowkey为用户ID
        val put = new Put(Bytes.toBytes(id))

        //todo 得到这行的be_tags中的所有数据
        get.addFamily(Bytes.toBytes("time_tags"))

        //todo 根据rowkey,列族名  查询出的结果
        val result: Result = table.get(get)

        //todo 定义个map，用于存放从HBASE取出的历史数据

        var map = Map[String, Double]()

        //todo 定义一个Boolean值，如果time_tags中有值，则为true，如果没有，则为false
        var boolean = true

        //todo 导入Java集合转为为scala的隐式转化，遍历上面查询出的结果集，并放到一个map集合中
        import scala.collection.JavaConversions._
        try

            for (cell <- result.listCells()) {
              val time_tags = Bytes.toString(CellUtil.cloneQualifier(cell))
              val time: Int = Bytes.toString(CellUtil.cloneValue(cell)).toInt
              map += time_tags -> time
            }
        catch {
          case _: NullPointerException => boolean = false
        }

        //todo 如果可以get到，表明该用户不是新用户，则进行如下操作
        if (boolean) {
          //todo 得到map集合中所有的key
          val keySet: Set[String] = {
            map.keySet
          }

          //todo 新的标签的所有的time_tags放到另外的set集合中
          val newKeySet: Set[String] = {
            i._2.keySet
          }

          //TODO 得到上面俩个set的交集
          val intersectSet: Set[String] = keySet.intersect(newKeySet)

          //todo 新建一个map集合，存新一天要放到HBASE的时间段标签
          var map1 = Map[String, Double]()

          //todo 如果在交集内，则从map中取出时间段相对应的次数与最新得到的次数相加
          for (s <- intersectSet) {
            val old_time = map.getOrElse(s, 0.0)

            val new_time: Double = i._2.getOrElse(s, 0.0)
            map1 += s -> (old_time + new_time)
          }


          //todo 新的标签中有，历史标签中没有的，放入到map1中
          for (s <- newKeySet.diff(keySet)) {
            val new_weight = i._2.getOrElse(s, 0.0)
            map1 += s -> new_weight
          }

          //TODO 生成的新的标签写入到time_tags列族中
          map1.foreach(x => {
            put.addColumn(Bytes.toBytes("time_tags"), Bytes.toBytes(x._1), Bytes.toBytes(x._2.toString))
          })

          //TODO 将map1根据次数排序后，整体写入到HBASE中
          val string_tags: String = map1.toList.sortBy(-_._2).map(x => {
            (x._1, x._2)
          }).toString()


          //todo 另外一份写入到user_tags
          put.addColumn(Bytes.toBytes("user_tags"), Bytes.toBytes("time_tags"), Bytes.toBytes(string_tags))

        } else {
          //todo 如果get不到，则说明此用户是新用户

          i._2.foreach(x => {
            put.addColumn(Bytes.toBytes("time_tags"), Bytes.toBytes(x._1), Bytes.toBytes(x._2.toString))
          })

          val string_tags: String = i._2.toList.sortBy(-_._2).map(x => {
            (x._1, x._2)
          }).toString()

          put.addColumn(Bytes.toBytes("user_tags"), Bytes.toBytes("time_tags"), Bytes.toBytes(string_tags))

        }

        //todo put 提交
        table.put(put)

      })

      table.close()
      connection.close()

    })


    spark.stop()

  }

}
