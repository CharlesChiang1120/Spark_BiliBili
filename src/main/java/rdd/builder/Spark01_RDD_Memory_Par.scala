package rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
  032.尚硅谷_SparkCore - 核心程式設計 - RDD - 建立 - 內存
 */

object Spark01_RDD_Memory_Par {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 創建 RDD
    // RDD 的併行度 ＆ 分區
    // makeRDD 第二個參數表示分區的數量
    // 第二個參數可以不傳遞，不傳會使用默認值
//    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    rdd.saveAsTextFile("output")

    rdd.collect().foreach(println)

    sc.stop()
  }
}
