package spark.demo

import org.apache.spark.sql.SparkSession
/**
 * 从Kafka的一个或多个主题中获取消息并计算搜索关键词及访问数量
 */
object StructuredStreamingKafkaFinal {

    def main(args: Array[String]): Unit = {
        //得到或创建SparkSession对象
        val spark = SparkSession
        .builder
        .appName("StructuredKafkaWordCount")
        .master("spark://centos01:7077")
        .getOrCreate()

        import spark.implicits._
        //从Kafka中获取数据并创建Dataset
        val lines = spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers",
            "centos01:9092,centos02:9092,centos03:9092")//Kafka集群的连接字符串
        .option("subscribe", "topictest")	//指定主题，多个使用逗号分隔
        .load()
        .selectExpr("CAST(value AS STRING)")	//使用SQL表达式将消息转为字符串
        .as[String]			//转为DataSet，便于后面进行转换操作

        //计算搜索词数量，根据value列分组（DataSet默认列名为value）
        val wordCounts = lines.map(line=>{
            val arr=line.split(",")
            arr(2)//取得搜索关键词
        })
        .groupBy("value")//根据value列分组
        .count()//计算每一组的数量
        .toDF("word","count")//转为DataFrame，便于计算

        //MySQL数据库连接信息
        val url = "jdbc:mysql://192.168.170.133:3306/word_count?useSSL=false"
        val username = "root"
        val password = "123456"

        //创建JDBC操作对象
        val writer = new MyWordCountJDBC(url, username, password)
        //输出流数据
        val query = wordCounts.writeStream
        .foreach(writer)//使用JDBC操作对象，将流查询的输出写入外部存储系统
        .outputMode("update")//只有结果流DataFrame/Dataset中更新过的行才会输出
//        .trigger(ProcessingTime("5 seconds"))//设置流查询的触发器。默认值是ProcessingTime(0)，将以尽可能快的速度运行查询
        .start()
        //等待查询终止
        query.awaitTermination()
    }

}
