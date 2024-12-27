package spark.demo.sql
import org.apache.spark.sql._
/**
 * Spark SQL加载JSON文件
 */
object SparkSQLJSONDemo{
  def main(args: Array[String]): Unit = {
    //创建或得到SparkSession
    val spark = SparkSession.builder()
      .appName("SparkSQLDataSource")
      .config("spark.sql.parquet.mergeSchema",true)
      .master("local[*]")
      .getOrCreate()

    /****1. 创建用户基本信息表*****/
    import spark.implicits._
    //创建用户信息Dataset集合
    val arr=Array(
      "{'name':'zhangsan','age':20}",
      "{'name':'lisi','age':18}"
    )
    val userInfo: Dataset[String] = spark.createDataset(arr)
    //将Dataset[String]转为DataFrame
    val userInfoDF = spark.read.json(userInfo)
    //创建临时视图user_info
    userInfoDF.createTempView("user_info")
    //显示数据
    userInfoDF.show()
    // +---+--------+
    // |age|    name|
    // +---+--------+
    // | 20|zhangsan|
    // | 18|    lisi|
    // +---+--------+

    /****2. 创建用户成绩表*****/
    //读取JSON文件
    val userScoreDF = spark.read.json("D:\\people\\people.json")
    //创建临时视图user_score
    userScoreDF.createTempView("user_score")
    userScoreDF.show()
    // +--------+-----+
    // |    name|score|
    // +--------+-----+
    // |zhangsan|   98|
    // |    lisi|   88|
    // |  wangwu|   95|
    //      +--------+-----+
    /****3. 根据name字段关联查询*****/
    val resDF=spark.sql("SELECT i.age,i.name,c.score FROM user_info i " +
      "JOIN user_score c ON i.name=c.name")
    resDF.show()
    // +---+--------+-----+
    // |age|    name|score|
    // +---+--------+-----+
    // | 20|zhangsan|   98|
    // | 18|    lisi|   88|
    // +---+--------+-----+
  }

}

