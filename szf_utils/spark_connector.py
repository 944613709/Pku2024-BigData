from pyspark.sql import SparkSession

# 创建 SparkSession
spark = SparkSession \
    .builder \
    .appName("PySparkExample") \
    .master("spark://192.168.88.100:10000") \
    .getOrCreate()

# 读取数据
df = spark.read.csv("hdfs://your_hdfs_path/data.csv")

# 执行 SQL 查询
df.createOrReplaceTempView("my_table")
result = spark.sql("SELECT * FROM my_table")

# 显示结果
result.show()

# 关闭 SparkSession
spark.stop()