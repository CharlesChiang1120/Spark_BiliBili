/*
  008.尚硅谷_Spark框架 - 快速上手 - WordCount - 不同的實現
 */
package bilibili.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_WordCount {
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
    (Hello,1)(World,1)(Hello,1)(Spark,1)(Hello,1)(World,1)(Hello,1)(Spark,1)(Hello,4)
    (World,2)
    (Spark,2)
     */
    val wordToOne = words.map{word =>(word, 1)}

    /*
    (Hello,CompactBuffer((Hello,1), (Hello,1), (Hello,1), (Hello,1)))
    (World,CompactBuffer((World,1), (World,1)))
    (Spark,CompactBuffer((Spark,1), (Spark,1)))
     */
    val wordGroup = wordToOne.groupBy(t => t._1)


    /*
    (Hello,4)
    (World,2)
    (Spark,2)
     */
    val wordToCount = wordGroup.map{
      case (word, list) =>
        list.reduce(
          (t1, t2) => {
            (t1._1, t1._2+t2._2)
          }
        )
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
