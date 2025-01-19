package rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

/*
  033.尚硅谷_SparkCore - 核心程式設計 - RDD - 建立 - 檔案
 */

object Spark02_RDD_File_Par {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 創建 RDD
    val rdd = sc.textFile("data/1.txt",3)
    rdd.saveAsTextFile("output")
    rdd.collect().foreach(println)

    sc.stop()
  }
}
