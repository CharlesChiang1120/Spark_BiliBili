package rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

/*
  033.尚硅谷_SparkCore - 核心程式設計 - RDD - 建立 - 檔案
 */

object Spark02_RDD_File1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 創建 RDD
    // 從文件中創建 RDD
    // wholeTextFiles: 以文件為單位讀取數據
    // 讀取的結果表示為元組，第一個元素表示文件路徑，第二個元素表示文件內容
    val rdd = sc.wholeTextFiles("data")
    rdd.collect().foreach(println)

    sc.stop()
  }
}
