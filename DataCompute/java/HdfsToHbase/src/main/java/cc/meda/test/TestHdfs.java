package cc.meda.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

/**
 * 测试读取hdfs文件
 * @author Yurisues 16-10-28 下午4:16
 */

public class TestHdfs {
    public static void main(String[] args){
        try {
            String dsf = "hdfs://CDH-0:8020/data/disk1/logdata/minik_machine/user_action/2016/2016-10-21min.log";
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(URI.create(dsf), conf);
            FSDataInputStream hdfsInStream = fs.open(new Path(dsf));

            byte[] ioBuffer = new byte[1024];
            int readLen = hdfsInStream.read(ioBuffer);
            while(readLen!=-1) {
                System.out.write(ioBuffer, 0, readLen);
                readLen = hdfsInStream.read(ioBuffer);
            }
            hdfsInStream.close();
            fs.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
