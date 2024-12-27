package spark.demo.rdd

import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/**
 * 向HBase表写入数据
 */
object SparkWriteHBase2 {
  def main(args: Array[String]): Unit = {
    //创建SparkConf对象，存储应用程序的配置信息
    val conf = new SparkConf()
    conf.setAppName("SparkWriteHBase2")
    conf.setMaster("local[*]")
    //创建SparkContext对象
    val sc = new SparkContext(conf)

    //1. 设置配置信息
    //创建Hadoop JobConf对象
    val jobConf = new JobConf()
    //设置ZooKeeper集群地址
    jobConf.set("hbase.zookeeper.quorum","192.168.170.133")
    //设置ZooKeeper连接端口，默认为2181
    jobConf.set("hbase.zookeeper.property.clientPort", "2181")
    //指定输出格式
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    //指定表名
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,"student")

    //2. 构建需要写入的RDD数据
    val initRDD = sc.makeRDD(
      Array(
        "005,王五,山东,23",
        "006,赵六,河北,20"
      )
    )

    //将RDD转换为(ImmutableBytesWritable, Put)类型
    val resultRDD: RDD[(ImmutableBytesWritable, Put)] = initRDD.map(
      _.split(",")
    ).map(arr => {
      val rowkey = arr(0)
      val name = arr(1)		//姓名
      val address = arr(2)	//地址
      val age = arr(3)		//年龄

      //创建Put对象
      val put = new Put(Bytes.toBytes(rowkey))
      put.addColumn(
        Bytes.toBytes("info"),	//列族
        Bytes.toBytes("name"),	//列名
        Bytes.toBytes(name)		//列值
      )
      put.addColumn(
        Bytes.toBytes("info"),	//列族
        Bytes.toBytes("address"),//列名
        Bytes.toBytes(address)) 	//列值
      put.addColumn(
        Bytes.toBytes("info"),	//列族
        Bytes.toBytes("age"),		//列名
        Bytes.toBytes(age)) 		//列值

      //拼接为元组返回
      (new ImmutableBytesWritable, put)
    })

    //3. 写入数据
    resultRDD.saveAsHadoopDataset(jobConf)
    sc.stop()
  }
}

