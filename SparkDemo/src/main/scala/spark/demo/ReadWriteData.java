package spark.demo;

import java.io.*;

/**
 * 按行读取搜索引擎用户搜索行为数据，写入指定日志文件中
 */
public class ReadWriteData {
    static String readFileName;//需要读取的文件路径
    static String writeFileName;//需要写入的文件路径

    public static void main(String args[]) {
        readFileName = args[0];//需要读取的文件路径
        writeFileName = args[1];//需要写入的文件路径
        try {
            //按行读取文件内容，并写入另一文件
            readFileByLines(readFileName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 按行读取文件内容
     * @param fileName 文件路径
     */
    public static void readFileByLines(String fileName) {
        FileInputStream fis = null;
        InputStreamReader isr = null;
        BufferedReader br = null;
        String tempString = null;
        try {
            fis = new FileInputStream(fileName);
            // 从文件系统中的某个文件中获取字节
            isr = new InputStreamReader(fis, "UTF8");
            br = new BufferedReader(isr);
            int count = 0;
            //读取文件内容，一次读一整行。tempString表示读到的一整行内容
            while ((tempString = br.readLine()) != null) {
                count++;//记录行号（当前读到第几行）
                Thread.sleep(300);//睡眠300毫秒
                //输出行号及该行的内容
                System.out.println("行:" + count + ">>>>>>>>" + tempString);
                //调用写入方法，将读取到的行内容写入指定文件
                writeFile(writeFileName, tempString);
            }
            isr.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (isr != null) {
                try {
                    isr.close();
                } catch (IOException e) {
                }
            }
        }
    }
    /**
     * 写入文件内容
     * @param fileName 文件路径
     * @param content 要写入的内容
     */
    public static void writeFile(String fileName, String content) {
        BufferedWriter out = null;
        try {
            out = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(fileName, true)));
            //写入内容
            out.write(content+"\n");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
