package cn.com.snsoft.user_tags

import java.io.File

import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.sql.SparkSession


/**
  * 用户基本属性标签，合并并写入HBASE
  *cn.com.snsoft.user_tags.mkBasicTags
  */
object mkBasicTags {
  def main(args: Array[String]): Unit = {
    //todo 参数控制
    if (args.length != 1) {
      println(
        """
          |mkTags
          |参数：分区日期
        """.stripMargin)
      sys.exit()
    }


    //todo 指定hive的源数据库
    val warehouseLocation = new File("jdbc:mysql://114.247.63.163:3306/hive").getAbsolutePath

    //todo 创建sparksession

    val spark: SparkSession = SparkSession.builder()
      .appName("mkBasicTags")
      //.master("spark://114.247.63.163:7337")
      //.master("local[1]")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport() //TODO 开启支持hive
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") //todo 采用KRYO序列化
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN") //设置日志输出级别


    //todo 指定数据库
    spark.sql("use snsoft_blc")

    //todo 操作fdm_blc_member表
    val member_DF = spark.sql(
      s"""
select * from fdm_blc_member where dt = ${args(0)}
      """.stripMargin)

    //todo hbase 中的表名
    val hbaseTableName = "snsoft_blc:blc_user"


    member_DF.rdd.foreachPartition(iter => {

      //todo 建立HBASE连接
      val hconf = HBaseConfiguration.create()
      hconf.set("hbase.zookeeper.quorum", "cdh1,cdh2,cdh3")
      hconf.set("hbase.zookeeper.property.clientPort", "2181")
      hconf.set("hbase.defaults.for.version.skip", "true")

      //TODO 在每个分区中建立连接
      val connection: Connection = ConnectionFactory.createConnection(hconf)
      val tableName: TableName = TableName.valueOf(hbaseTableName)

      val table: Table = connection.getTable(tableName)

      iter.foreach(row => {
        //todo 用户ID
        val id = row.getAs[String]("id")

        //todo 用户基本属性标签
        val basic_Tags: Map[String, String] = basic_tags.mkTags(row)

        //todo 写入HBASE，并指定每行的rowkey为用户ID
        val put: Put = new Put(Bytes.toBytes(id))

        //todo map.get时，存入HBASE中的数据类型为some类型，换成getorelse后变成string
        basic_Tags.keys.foreach(i => {
          put.addColumn(Bytes.toBytes("user_tags"), Bytes.toBytes(i), Bytes.toBytes(basic_Tags.getOrElse(i, "null").toString))
        })

        //todo 提交
        table.put(put)
        // println(id, basic_Tags)

      })
      //todo 连接关闭
      connection.close()
    })

    //TODO 关闭资源
    spark.stop()

  }

}
