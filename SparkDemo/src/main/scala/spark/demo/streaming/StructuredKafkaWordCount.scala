package spark.demo.streaming

import org.apache.spark.sql.SparkSession
/**
 * 从Kafka的一个或多个主题中获取消息并计算单词数量
 */
object StructuredKafkaWordCount {
  def main(args: Array[String]): Unit = {

    //得到或创建SparkSession对象
    val spark = SparkSession
      .builder
      .appName("StructuredKafkaWordCount")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    //从Kafka中获取数据并创建Dataset
    val lines = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers",
        "centos01:9092,centos02:9092,centos03:9092")
      .option("subscribe", "topic1")		//指定主题，多个使用逗号分隔
      .load()
      .selectExpr("CAST(value AS STRING)")	//使用SQL表达式将消息转为字符串
      .as[String]			//转为Dataset，便于后面进行转换操作

    lines.printSchema()	//打印Schema信息
    // root
    // |-- value: string (nullable = true)

    //计算单词数量，根据value列分组
    val wordCounts = lines.flatMap(_.split(" ")).groupBy("value").count()

    //启动查询，打印结果到控制台
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .option("checkpointLocation", "hdfs://centos01:9000/kafka-checkpoint")	    //指定检查点目录
      .start()

    //等待查询终止
    query.awaitTermination()
  }

}

