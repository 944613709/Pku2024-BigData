package spark.demo.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
/**
 * Spark Streaming实时单词计数，多个单词以空格分割
 */
object StreamingWordCount {

  def main(args: Array[String]) {
    //创建SparkConf
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("NetworkWordCount")
    //创建StreamingContext，设置批次间隔为1秒
    val ssc = new StreamingContext(conf, Seconds(1))
    //设置检查点目录，因为需要用检查点记录历史批次处理的结果数据
    ssc.checkpoint("hdfs://centos01:9000/spark-ck")

    //创建输入DStream，从Socket中接收数据
    val lines: ReceiverInputDStream[String] =
      ssc.socketTextStream("centos01", 9999)
    //根据空格把接收到的每一行数据分割成单词
    val words = lines.flatMap(_.split(" "))
    //将每个单词转换为(word,1)形式的元组
    val wordCounts = words.map(x => (x, 1))

    //更新每个单词的数量，实现按批次累加
    val result:DStream[(String,Int)]=
      wordCounts.updateStateByKey(updateFunc)
    //默认打印DStream中每个RDD中的前10个元素到控制台
    result.print()

    ssc.start() 				//启动计算
    ssc.awaitTermination() 	//等待计算结束
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

