/*
  007.尚硅谷_Spark框架 - 快速上手 - WordCount - 功能實現
 */
package bilibili.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_WordCount {
  def main(args: Array[String]): Unit = {

    // TODO 建立與 Spark 連接
    // JDBC: Connection
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    // TODO 執行業務操作
    /*
    Hello World
    Hello Spark
    Hello World
    Hello Spark
     */
    val lines: RDD[String] = sc.textFile("data")

    /*
    Hello
    World
    Hello
    Spark
    Hello
    World
    Hello
    Spark
     */
    val words: RDD[String] = lines.flatMap(_.split(" "))

    /*
    (Hello,CompactBuffer(Hello, Hello, Hello, Hello))
    (World,CompactBuffer(World, World))
    (Spark,CompactBuffer(Spark, Spark))
     */
    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)


    /*
    (Hello,4)
    (World,2)
    (Spark,2)
     */
    val wordToCount = wordGroup.map{
      case (word, list) =>
        (word, list.size)
    }

    /*
    (Hello,4)
    (World,2)
    (Spark,2)
     */
    val array: Array[(String, Int)] = wordToCount.collect()
    array.foreach(println)

    // 關閉連接
    sc.stop( )
  }
}
