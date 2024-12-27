package spark.demo.rdd

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.ConnectionFactory
/**
 * 向HBase表写入数据
 */
object SparkWriteHBase {
  def main(args: Array[String]): Unit = {
    //创建SparkConf对象，存储应用程序的配置信息
    val conf = new SparkConf()
    conf.setAppName("SparkWriteHBase")
    conf.setMaster("local[*]")
    //创建SparkContext对象
    val sc = new SparkContext(conf)

    //1. 构建需要添加的数据RDD
    val initRDD = sc.makeRDD(
      Array(
        "003,王五,山东,23",
        "004,赵六,河北,20"
      )
    )

    //2. 循环RDD的每个分区
    initRDD.foreachPartition(partition=> {
      //2.1 设置HBase配置信息
      val hbaseConf = HBaseConfiguration.create()
      //设置ZooKeeper集群地址
      hbaseConf.set("hbase.zookeeper.quorum","192.168.170.133")
      //设置ZooKeeper连接端口，默认为2181
      hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
      //创建数据库连接对象
      val conn = ConnectionFactory.createConnection(hbaseConf)
      //指定表名
      val tableName = TableName.valueOf("student")
      //获取需要添加数据的Table对象
      val table = conn.getTable(tableName)

      //2.2 循环当前分区的每行数据
      partition.foreach(line => {
        //分割每行数据，获取要添加的每个值
        val arr = line.split(",")
        val rowkey = arr(0)
        val name = arr(1)
        val address = arr(2)
        val age = arr(3)

        //创建Put对象
        val put = new Put(Bytes.toBytes(rowkey))
        put.addColumn(
          Bytes.toBytes("info"),		//列族名
          Bytes.toBytes("name"),		//列名
          Bytes.toBytes(name)		//列值
        )
        put.addColumn(
          Bytes.toBytes("info"),		//列族名
          Bytes.toBytes("address"),	//列名
          Bytes.toBytes(address)) 	//列值
        put.addColumn(
          Bytes.toBytes("info"),		//列族名
          Bytes.toBytes("age"),		//列名
          Bytes.toBytes(age)) 		//列值

        //执行添加
        table.put(put)
      })
    })

  }
}

