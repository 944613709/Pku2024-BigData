package spark.demo

import org.apache.spark.sql.{ForeachWriter, Row}
import java.sql._

/**
 * JDBC操作类
 */
class MyWordCountJDBC(url: String, username: String, password: String) extends ForeachWriter[Row] {
    //命令执行对象
    var statement: Statement = _
    //存储结果集
    var resultSet: ResultSet = _
    //数据库连接对象
    var connection: Connection = _

    /**
     * 打开数据库连接
     */
    override def open(partitionId: Long, version: Long): Boolean = {
        //加载驱动
        Class.forName("com.mysql.cj.jdbc.Driver")
        //获得数据库连接对象Connection
        connection = DriverManager.getConnection(url, username, password)
        //获得命令执行对象Statement
        statement = connection.createStatement()
        return true
    }

    /**
     * 操作数据库表数据
     * 只有当open()方法返回true时，该方法才会被调用
     * @param value 需要写入数据库的一整行数据
     */
    override def process(value: Row): Unit = {

//        val titleName = value.getAs[String]("titleName").replaceAll("[\\[\\]]", "")
        //获取传入的单词名称
        val word = value.getAs[String]("word")
        //获取传入的单词数量
        val count = value.getAs[Long]("count")

        //查询SQL。查询表中某个单词是否存在
        val querySql = "select 1 from word_count where word = '" + word + "'"
        //更新SQL。更新表中某个单词对应的数量
        val updateSql = "update word_count set count = " + count + " where word = '" + word + "'"
        //插入SQL。向表中添加一条数据
        val insertSql = "insert into word_count(word,count) values('" + word + "'," + count + ")"

        try {
            //执行查询
            var resultSet = statement.executeQuery(querySql)
            //如果有数据，则更新。否则则添加
            if (resultSet.next()) {
                statement.executeUpdate(updateSql)//执行更新
            } else {
                statement.execute(insertSql)//执行添加
            }
        } catch {
            case ex: Exception => {
                println(ex.getMessage)
            }
        }
    }

    /**
     * 关闭数据库连接
     */
    override def close(errorOrNull: Throwable): Unit = {
        if (statement != null) {
            statement.close()
        }
        if (connection != null) {
            connection.close()
        }
    }

}
