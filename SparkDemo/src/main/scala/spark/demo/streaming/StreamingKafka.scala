package spark.demo.streaming

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{KafkaUtils, LocationStrategies}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

/**
 * Spark Streaming整合Kafka实现单词计数
 */
object StreamingKafka{
  //所有org包名只输出ERROR级别的日志，如果导入其他包，那么只需要再新创建一行写入包名即可
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args:Array[String]){
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("StreamingKafkaWordCount")

    //创建Spark Streaming上下文，并以1秒内收到的数据作为一个批次
    val ssc = new StreamingContext(conf, Seconds(1))
    //设置检查点目录，因为需要用检查点记录历史批次处理的结果数据
    ssc.checkpoint("hdfs://centos01:9000/spark-ck")

    //设置输入流的Kafka主题，可以设置多个
    val kafkaTopics = Array("topictest")

    //Kafka配置属性
    val kafkaParams = Map[String, Object](
      //Kafka Broker服务器的连接地址
      "bootstrap.servers" -> "centos01:9092,centos02:9092,centos03:9092",
      //设置反序列化key的程序类，与生产者对应
      "key.deserializer" -> classOf[StringDeserializer],
      //设置反序列化value的程序类，与生产者对应
      "value.deserializer" -> classOf[StringDeserializer],
      //设置消费者组ID，ID相同的消费者属于同一个消费者组
      "group.id" -> "1",
      //Kafka不自动提交偏移量（默认为true），由Spark管理
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    //创建输入DStream
    val inputStream: InputDStream[ConsumerRecord[String, String]] =
      KafkaUtils.createDirectStream[String, String](
    ssc,
    LocationStrategies.PreferConsistent,
    Subscribe[String, String](kafkaTopics, kafkaParams)
    )

    //对接收到的一个DStream进行解析，取出消息记录的key和value
    val linesDStream = inputStream.map(record => (record.key, record.value))
    //默认情况下，消息内容存放在value中，取出value的值
    val wordsDStream = linesDStream.map(_._2)
    val word = wordsDStream.flatMap(_.split(" "))
    val pair = word.map(x => (x,1))

    //更新每个单词的数量，实现按批次累加
    val result:DStream[(String,Int)]= pair.updateStateByKey(updateFunc)
    //默认打印DStream中每个RDD中的前10个元素到控制台
    result.print()

    ssc.start
    ssc.awaitTermination
  }

  /**
   * 定义状态更新函数，按批次累加单词数量
   * @param values 当前批次单词出现的次数，相当于Seq(1, 1, 1)
   * @param state  上一批次累加的结果，因为有可能没有值，所以用Option类型
   */
  val updateFunc=(values:Seq[Int],state:Option[Int])=>{
    val currentCount=values.foldLeft(0)(_+_)//累加当前批次单词的数量
    val previousCount= state.getOrElse(0)	//获取上一批次单词的数量，默认值为0
    Some(currentCount+previousCount)		//求和
  }

}

