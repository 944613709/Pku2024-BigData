package spark.demo.sql

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * SparkSQL读取HDFS数据，并合并数据Schema信息
 */
object SparkSQLMergeSchemaDemo {
  def main(args: Array[String]): Unit = {
    //创建或得到SparkSession
    val spark = SparkSession.builder()
      .appName("SparkSQLDataSource")
      .config("spark.sql.parquet.mergeSchema",true)
      .master("local[*]")
      .getOrCreate()

    //导入隐式转换
    import spark.implicits._

    //创建List集合，存储姓名和年龄
    val studentList=List(("jock",22),("lucy",20))
    //将集合转为DataFrame，并指定列名为name和age
    val studentDF = spark.sparkContext
    .makeRDD(studentList)
      .toDF("name", "age")
    //将DataFrame写入HDFS的/students目录
    studentDF.write.mode(SaveMode.Append)
      .parquet("hdfs://centos01:9000/students")

    //创建List集合，存储姓名和成绩
    val studentList2=List(("tom",98),("mary",100))
    //将集合转为DataFrame，并指定列名为name和grade
    val studentDF2 = spark.sparkContext
    .makeRDD(studentList2)
      .toDF("name", "grade")
    //将DataFrame写入HDFS的/students目录（写入模式为Append）
    studentDF2.write.mode(SaveMode.Append)
      .parquet("hdfs://centos01:9000/students")

    //读取HDFS目录/students的Parquet文件数据，并合并Schema
    val mergedDF = spark.read.option("mergeSchema", "true")
      .parquet("hdfs://centos01:9000/students")
    //输出Schema信息
    mergedDF.printSchema()
    //输出数据内容
    mergedDF.show()
  }
}

