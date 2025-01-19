package rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
  032.尚硅谷_SparkCore - 核心程式設計 - RDD - 建立 - 內存
 */

object Spark01_RDD_Memory_Par1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 創建 RDD
    // [1, 2], [3, 4]
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5), 3)
    rdd.saveAsTextFile("output")

    sc.stop()
  }
}
