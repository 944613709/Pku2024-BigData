package spark.demo.rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
/**
 * Spark读取HBase表数据
 */
object SparkReadHBase {
  def main(args: Array[String]): Unit = {
    //创建SparkConf对象，存储应用程序的配置信息
    val conf = new SparkConf()
    conf.setAppName("SparkReadHBase")
    conf.setMaster("local[*]")
    //创建SparkContext对象
    val sc = new SparkContext(conf)

    //1. 设置HBase配置信息
    val hbaseConf = HBaseConfiguration.create()
    //设置ZooKeeper集群地址
    hbaseConf.set("hbase.zookeeper.quorum","192.168.170.133")
    //设置ZooKeeper连接端口，默认为2181
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    //指定表名
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "student")

    //2. 读取HBase表数据并转化成RDD
    val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      hbaseConf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )

    //3. 输出RDD中的数据到控制台
    hbaseRDD.foreach{ case (_ ,result) =>
      //获取行键
      val key = Bytes.toString(result.getRow)
      //通过列族和列名获取列值
      val name = Bytes.toString(result.getValue("info".getBytes,"name".getBytes))
      val gender = Bytes.toString(result.getValue("info".getBytes,"address".getBytes))
      val age = Bytes.toString(result.getValue("info".getBytes,"age".getBytes))
      println("行键:"+key+"\t姓名:"+name+"\t地址:"+gender+"\t年龄:"+age)
    }

  }
}

