package randomTest

import org.apache.spark.sql.SparkSession

object SparkApp {
  def main(args: Array[String]): Unit = {
    // 创建 SparkSession
    val spark = SparkSession.builder()
      .appName("Spark IntelliJ Test")
      .master("local[*]") // 使用本地模式
      .getOrCreate()

    // 创建一个简单的 DataFrame
    val data = Seq(("Alice", 25), ("Bob", 30), ("Cathy", 22))
    val df = spark.createDataFrame(data).toDF("Name", "Age")

    // 显示 DataFrame 内容
    df.show()

    // 计算平均年龄
    df.groupBy().avg("Age").show()

    // 停止 SparkSession
    spark.stop()
  }
}
