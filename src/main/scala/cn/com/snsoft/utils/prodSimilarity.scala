package cn.com.snsoft.utils

import org.apache.spark.rdd.RDD

/**
  * prodSimilarity
  * inParameter：userAndProdRdd （uesrId，prod_id）
  * outParameter：prodId,List(prodId * 6)
  *
  *
  */
object prodSimilarity {
  def mkProSimilarity(userAndProdRdd: RDD[(String, String)]): RDD[(String, List[String])] = {

    val joinRdd: RDD[((String, String), Int)] = userAndProdRdd.join(userAndProdRdd).map(x => {
      (x._2, 1)
    })
    val reduceRdd: RDD[((String, String), Int)] = joinRdd.reduceByKey(_ + _)

    val filterRdd1: RDD[((String, String), Int)] = reduceRdd.filter(x => x._1._1 == x._1._2)

    val filterRdd2: RDD[((String, String), Int)] = reduceRdd.filter(x => x._1._1 != x._1._2)

    val itemFrequence: RDD[(String, Int)] = filterRdd1.map(x => {
      (x._1._1, x._2)
    })

    val itemIdRdd: RDD[(String, (String, String, Int))] = filterRdd2.map(x => {
      (x._1._1, (x._1._1, x._1._2, x._2))
    })


    val joinRdd2: RDD[(String, ((String, String, Int), Int))] = itemIdRdd.join(itemFrequence)

    //todo (item_i,((item_i,item_j,freqence_ij),fequence_i))
    val rdd3: RDD[(String, (String, String, Int, Int))] = joinRdd2.map(x => {
      (x._2._1._2, (x._2._1._1, x._2._1._2, x._2._1._3, x._2._2))
    }
    )
    //TODO (item_j,((item_i,item_j,freqence_ij,fequence_i),frequence_j)
    val rdd4: RDD[(String, ((String, String, Int, Int), Int))] = rdd3.join(itemFrequence)


    //TODO (item_i,item_j,freqence_ij,fequence_i,fequence_j)
    val rdd5: RDD[(String, String, Int, Int, Int)] = rdd4.map(x => {
      (x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._4, x._2._2)
    })

    val res: RDD[(String, (String, Double))] = rdd5.map(x => {
      (x._2, (x._1, x._3 / scala.math.sqrt(x._4 * x._5)))
    })

    //todo 得到与每个物品相似度排名前五的物品

    val prodAndProdId: RDD[(String, List[String])] = res.groupByKey().map(x => {
      (x._1, x._2.toList.sortBy(-_._2).take(6).map(p => {
        p._1
      }))
    })


    prodAndProdId

  }


}
