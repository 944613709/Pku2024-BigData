package spark.demo.streaming

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * Structured Streaming单词计数
 */
object StructuredNetworkWordCount {

  def main(args: Array[String]): Unit = {
    //创建本地SparkSession
    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master("local[*]")
      .getOrCreate()

    //设置日志级别为WARN
    spark.sparkContext.setLogLevel("WARN")
    //导入SparkSession对象中的隐式转换
    import spark.implicits._

    //从Socket连接中获取输入流数据创建DataFrame
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "centos01")
      .option("port", 9999)
      .load()

    //分割每行数据为单词
    val words: Dataset[String] = lines.as[String].flatMap(_.split(" "))
    //计算单词数量（value为默认的列名）
    val wordCounts: DataFrame = words.groupBy("value").count()

    //输出计算结果，3种模式：
    //complete 所有内容都输出
    //append 新增的行才输出
    //update 更新的行才输出
    val query = wordCounts.writeStream
    .outputMode("complete")
      .format("console")
      .start()

    //等待查询终止
    query.awaitTermination()
  }
}

