package rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
  032.尚硅谷_SparkCore - 核心程式設計 - RDD - 建立 - 內存
 */

object Spark01_RDD_Memory {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 創建 RDD
    // 從內存中創建 RDD
    val seq = Seq[Int](1, 2, 3, 4)

    // parallelize: 並行
//    val rdd: RDD[Int] = sc.parallelize(seq)
    // makeRdd 方法在底層實現時其實就是調用 parallelize 方法
    val rdd: RDD[Int] = sc.makeRDD(seq)

    rdd.collect().foreach(println)

    sc.stop()
  }
}
