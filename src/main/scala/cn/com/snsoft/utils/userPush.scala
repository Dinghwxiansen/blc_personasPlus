package cn.com.snsoft.utils

import java.util.Properties

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._

import scala.collection.mutable

object userPush {
  def main(args: Array[String]): Unit = {

    //todo 创建sparksession

    val spark: SparkSession = SparkSession.builder()
      .appName("SparkSQL2Local")
      //.master("spark://114.247.63.163:7337")
      //.master("local[1]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") //todo 采用KRYO序列化
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN") //设置日志输出级别

    val sc = spark.sparkContext
    //todo 要操作的HBASE中的表
    val hbaseTable = "snsoft_blc:blc_user"

    //todo 参数配置
    //todo 建立HBASE连接
    val hconf = HBaseConfiguration.create()
    hconf.set("hbase.zookeeper.quorum", "cdh1,cdh2,cdh3")
    hconf.set("hbase.zookeeper.property.clientPort", "2181")
    hconf.set("hbase.defaults.for.version.skip", "true")
    hconf.set(TableInputFormat.INPUT_TABLE, hbaseTable)


    val hbaseRdd: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(hconf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

    //todo 导入隐式转化
    import scala.collection.JavaConversions._
    //TODO 导入set集合，map集合的隐式转换
    //import scala.collection.mutable.{Map, Set}

    //Todo 取出用户ID，用户浏览商品,以及对应权重
    val userAndProdRdd: RDD[(String, mutable.Map[String, String])] = hbaseRdd.map(x => {
      val rowKey: String = Bytes.toString(x._1.get())
      val map: mutable.Map[String, String] = x._2.getFamilyMap(Bytes.toBytes("prod_tags")).map(i => {
        (Bytes.toString(i._1), Bytes.toString(i._2))
      })
      (rowKey, map)

    })


    val filter: RDD[(String, mutable.Map[String, String])] = userAndProdRdd.filter(_._2.nonEmpty)

    val userAndProd: RDD[(String, collection.Set[String])] = filter.mapPartitions(itea => {
      itea.map(x => {
        (x._1, x._2.keySet.map(p => {
          p.substring(5, p.length)
        }))
      })
    })

    val userAndProdRdd2: RDD[(String, String)] = userAndProd.flatMap(x => {
      x._2.map(p => {
        (x._1, p)
      })
    })
    //userAndProdRdd5.collect().foreach(println)

    val similarity: RDD[(String, List[String])] = prodSimilarity.mkProSimilarity(userAndProdRdd2)


    val toList: List[(String, List[String])] = similarity.collect().toList

    /*    println(toList)
        println("111111111111111111111")*/

    val broadcast: Broadcast[List[(String, List[String])]] = sc.broadcast(toList)

    //TODO  用户与其权重最高的商品
    val userAndProd3: RDD[(String, String)] = filter.mapPartitions(itea => {
      itea.map(x => {
        val prodId: String = x._2.toList.sortBy(_._2.toDouble).take(1).map(_._1).get(0)
        (x._1, prodId.substring(5, prodId.length))

      })
    })


    val userAndProd4: RDD[(String, List[String])] = userAndProd3.mapPartitions(itea => {
      val map: Map[String, List[String]] = broadcast.value.toMap[String, List[String]]
      itea.map(x => {

        val list: List[String] = map.getOrElse(x._2, List[String]())

        (x._1, list)

      })
    })

    /*    val userAndProd5: RDD[(String, String, String, Int)] = userAndProd4.flatMap(x => {
          x._2.map(p => {
            (getRandomId.getRandomId(32), x._1, p, 1)
          })
        })*/

/*
    val nowDate: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)

    val time: Timestamp = sql.Timestamp.valueOf(nowDate)
*/


    val userAndProd6: RDD[Row] = userAndProd4.flatMap(x => {
      x._2.map(p => {
        Row(getRandomId.getRandomId(32), x._1, p, 1)
      })
    })


    //todo 创建 schema信息
    val schema: StructType = StructType(Array(
      StructField("ID", StringType, nullable = true),
      StructField("MEMBER_ID", StringType, nullable = false),
      StructField("PROD_ID", StringType, nullable = true),
      StructField("STATUS", IntegerType, nullable = true)
      /*StructField("CREATE_TIME", DateType, nullable = true),
      StructField("UPDATE_TIME", DateType, nullable = true)*/
    ))

    val uAndPDF: DataFrame = spark.createDataFrame(userAndProd6,schema)


    /*
        val date: Date = new Date()

        val date1: sql.Date = new sql.Date(date.getTime)

        val userAndProd7: RDD[(String, String, String, Int, Date, Date)] = userAndProd4.flatMap(x => {
          x._2.map(p => {
            (getRandomId.getRandomId(32), x._1, p, 1, date1, date1)
          })
        })
    */

    //userAndProd6.take(10).foreach(println)


    //userAndProd4.collect().foreach(println)


    val properties: Properties = new java.util.Properties()
    properties.put("user", "C##blcadmin")
    properties.put("password", "blcadmin")
    //properties.setProperty("url","jdbc:oracle:thin:@10.1.133.102:1521:orcl")
    properties.put("characterEncoding", "utf8")
    Class.forName("oracle.jdbc.driver.OracleDriver").newInstance()

    //todo 导入隐式转化

    uAndPDF.show()

    uAndPDF.write.mode(SaveMode.Append).jdbc("jdbc:oracle:thin:@10.1.133.102:1521:orcl", "BLC_PROD_PUSH", properties)


    /*uAndPDf.registerTempTable("userAndProd")

    //val sql = "SELECT * FROM userAndProd"

    //sqlContext.sql(sql).write.mode(SaveMode.Append).jdbc("jdbc:oracle:thin:@10.1.133.102:1521:orcl", "BLC_PROD_PUSH", properties)


    org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.saveTable(uAndPDf, "jdbc:oracle:thin:@39.106.203.2:1521:orcl", "BLC_PROD_PUSH", properties)
    //uAndPDf.write.jdbc()

    JdbcUtils.saveTable(uAndPDf,)*/
    println("当日推荐完成")

    sc.stop()
    spark.stop()


  }

}