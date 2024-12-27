package spark.demo.sql

import org.apache.spark.sql.SparkSession
/**
 * Spark SQL操作Hive
 */
object SparkSQLHiveDemo{
  def main(args: Array[String]): Unit = {
    //创建SparkSession对象
    val spark = SparkSession
      .builder()
      .appName("Spark Hive Demo")
      .enableHiveSupport()//开启Hive支持
      .getOrCreate()

    //创建表students
    spark.sql("CREATE TABLE IF NOT EXISTS students (name STRING, age INT) " + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'")
    //导入数据到表students
    spark.sql("LOAD DATA LOCAL INPATH '/home/hadoop/students.txt' " +
      "INTO TABLE students")
    // 使用HiveQL查询表students的数据
    spark.sql("SELECT * FROM students").show()
  }
}

