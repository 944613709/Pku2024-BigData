package spark.demo.streaming

import java.sql.Timestamp
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Structured Streaming窗口聚合单词计数
 * 每隔5秒计算前10秒的单词数量
 */
object StructuredNetworkWordCountWindowed {

  def main(args: Array[String]) {

    //得到或创建SparkSession对象
    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCountWindowed")
      .master("local[*]")
      .getOrCreate()

    //设置日志级别为WARN
    spark.sparkContext.setLogLevel("WARN")
    //滑动间隔必须小于等于窗口长度。若不设置滑动间隔，则默认等于窗口长度
    val windowDuration = "10 seconds"	//窗口长度
    val slideDuration = "5 seconds"		//滑动间隔
    import spark.implicits._

    //从Socket连接中获取输入流数据并创建DataFrame
    //从网络中接收的每一行数据都带有一个时间戳（数据产生时间），用于确定该行数据所属的窗口
    val lines = spark.readStream
    .format("socket")
      .option("host", "centos01")
      .option("port", 9999)
      .option("includeTimestamp", true)	//指定包含时间戳
      .load()
    lines.printSchema()
    // root
    // |-- value: string (nullable = true)
    // |-- timestamp: timestamp (nullable = true)

    //将每一行分割成单词，保留时间戳（单词产生的时间）
    val words = lines.as[(String, Timestamp)].flatMap(line =>
      line._1.split(" ").map(word => (word, line._2))
    ).toDF("word", "timestamp")

    //将数据按窗口和单词分组，并计算每组的数量
    val windowedCounts = words.groupBy(
    window($"timestamp", windowDuration, slideDuration),
    $"word"
    ).count().orderBy("window")

    //执行查询，并将窗口的单词数量打印到控制台
    val query = windowedCounts.writeStream
    .outputMode("complete")
      .format("console")
      .option("truncate", "false")	//如果输出太长是否截断（默认: true）
      .start()
    //等待查询终止
    query.awaitTermination()
  }
}

