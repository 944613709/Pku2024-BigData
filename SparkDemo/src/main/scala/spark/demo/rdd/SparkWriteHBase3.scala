package spark.demo.rdd

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Spark 批量写入数据到HBase
 */
object SparkWriteHBase3 {
  def main(args: Array[String]): Unit = {
    //创建SparkConf对象，存储应用程序的配置信息
    val conf = new SparkConf()
    conf.setAppName("SparkWriteHBase3")
    conf.setMaster("local[*]")
    //创建SparkContext对象
    val sc = new SparkContext(conf)

    //1. 设置HDFS和HBase配置信息
    val hadoopConf = new Configuration()
    hadoopConf.set("fs.defaultFS", "hdfs://192.168.170.133:9000")
    val fileSystem = FileSystem.get(hadoopConf)
    val hbaseConf = HBaseConfiguration.create(hadoopConf)
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
    //获取操作数据库的Admin对象
    val admin = conn.getAdmin()

    //2. 添加数据前的判断
    //如果HBase表不存在，就创建一个新表
    if (!admin.tableExists(tableName)) {
      val desc = new HTableDescriptor(tableName)	//表名
      val hcd = new HColumnDescriptor("info")		//列族
      desc.addFamily(hcd)
      admin.createTable(desc)		//创建表
    }
    //如果存放HFile文件的HDFS目录已经存在，就删除
    if(fileSystem.exists(new Path("hdfs://192.168.170.133:9000/tmp/hbase"))) {
      fileSystem.delete(new Path("hdfs://192.168.170.133:9000/tmp/hbase"), true)
    }

    //3. 构建需要添加的RDD数据
    //初始数据
    val initRDD = sc.makeRDD(
      Array(
        "rowkey:007,name:王五",
        "rowkey:007,address:山东",
        "rowkey:007,age:23",
        "rowkey:008,name:赵六",
        "rowkey:008,address:河北",
        "rowkey:008,age:20"
      )
    )
    //数据转换
    //转换为(ImmutableBytesWritable, KeyValue)类型的RDD
    val resultRDD: RDD[(ImmutableBytesWritable, KeyValue)] = initRDD.map(
      _.split(",")
    ).map(arr => {
      val rowkey = arr(0).split(":")(1)		//rowkey
      val qualifier = arr(1).split(":")(0)	//列名
      val value = arr(1).split(":")(1)		//列值

      val kv = new KeyValue(
      Bytes.toBytes(rowkey),
      Bytes.toBytes("info"),
      Bytes.toBytes(qualifier),
      Bytes.toBytes(value)
      )
      //构建(ImmutableBytesWritable, KeyValue)类型的元组返回
      (new ImmutableBytesWritable(Bytes.toBytes(rowkey)), kv)
    })

    //4. 写入数据
    //在HDFS中生成HFile文件
    resultRDD.saveAsNewAPIHadoopFile(
      "hdfs://192.168.170.133:9000/tmp/hbase",
      classOf[ImmutableBytesWritable], //对应RDD元素中的key
      classOf[KeyValue], //对应RDD元素中的value
      classOf[HFileOutputFormat2],
      hbaseConf
    )
    //加载HFile文件到HBase
    val bulkLoader = new LoadIncrementalHFiles(hbaseConf)
    val regionLocator = conn.getRegionLocator(tableName)
    bulkLoader.doBulkLoad(
      new Path("hdfs:	//192.168.170.133:9000/tmp/hbase"), //HFile文件位置
      admin, //操作HBase数据库的Admin对象
      table, //目标Table对象（包含表名）
      regionLocator //RegionLocator对象，用于查看单个HBase表的区域位置信息
    )
    sc.stop()
  }
}
