package rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
  033.尚硅谷_SparkCore - 核心程式設計 - RDD - 建立 - 檔案
 */

object Spark02_RDD_File {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 創建 RDD
    // 從文件中創建 RDD
//    val rdd: RDD[String] = sc.textFile("data/1.txt")
    val rdd = sc.textFile("data")
    rdd.collect().foreach(println)

    sc.stop()
  }
}
