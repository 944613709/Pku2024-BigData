package org.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * JDBC查询类
 */
public class JdbcUtil {
    //MySQL数据库连接信息
    static String url = "jdbc:mysql://192.168.170.133:3306/word_count?useSSL=false";
    static String username = "root";
    static String password = "123456";

    /**
     * 查询数据库结果数据
     * @return 结果数据
     */
    public Map<String, Object> queryResultData() {
        Connection conn = null;
        PreparedStatement pst = null;

        //创建搜索关键词集合
        List<String> words = new ArrayList<String>();
        //创建关键词数量集合
        List<Integer> counts = new ArrayList<Integer>();
        //创建返回结果Map（包含words和counts两个集合）
        Map<String, Object> resultMap = new HashMap<String, Object>();
        try {
            //加载驱动
            Class.forName("com.mysql.cj.jdbc.Driver");
            //获得数据库连接对象Connection
            conn = DriverManager.getConnection(url, username, password);
            //查询搜索数量最多的前10个搜索词
            String sql = "select word,count from word_count where 1=1 order by count desc limit 10";
            pst = conn.prepareStatement(sql);
            ResultSet rs = pst.executeQuery();
            while (rs.next()) {
                String word = rs.getString("word");//得到搜索词
                String count = rs.getString("count");//得到搜索数量

                words.add(word);//添加到搜索词集合中
                counts.add(Integer.parseInt(count));//添加到搜索数量集合中
            }
            //将words和counts两个集合添加到结果Map中
            resultMap.put("words", words);
            resultMap.put("counts", counts);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            //关闭连接，释放资源
            try {
                if (pst != null) {
                    pst.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return resultMap;//返回查询结果
    }

}
