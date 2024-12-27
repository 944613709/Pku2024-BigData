package spark.demo.sql

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/** 将RDD中的数据写入到MySQL* */

object SparkSQLJDBC {

  def main(args: Array[String]): Unit = {
    //创建或得到SparkSession
    val spark = SparkSession.builder()
      .appName("SparkSQLJDBC")
      .getOrCreate()
    //创建存放两条学生信息的RDD
    val studentRDD = spark.sparkContext.parallelize(
      Array("4 xiaoming 26", "5 xiaogang 27")
    ).map(_.split(" "))
    //通过StructType指定每个字段的schema
    val schema = StructType(
      List(
        StructField("id", IntegerType, true),
        StructField("name", StringType, true),
        StructField("age", IntegerType, true))
    )
    //将studentRDD映射为rowRDD，rowRDD中的每个元素都为一个Row对象
    val rowRDD = studentRDD.map(line =>
      Row(line(0).toInt, line(1).trim, line(2).toInt)
    )
    //建立rowRDD和schema之间的对应关系
    val studentDF = spark.createDataFrame(rowRDD, schema)
    //将结果追加到MySQL的student表中
    studentDF.write.mode("append") //保存模式为追加，即在原来的表中追加数据
      .format("jdbc")
      .option("url", "jdbc:mysql://192.168.170.133:3306/spark_db")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("dbtable", "student") //表名
      .option("user", "root")
      .option("password", "123456")
      .save()
  }

}
