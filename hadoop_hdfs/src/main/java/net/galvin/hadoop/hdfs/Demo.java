package net.galvin.hadoop.hdfs;

import net.galvin.hadoop.comm.utils.Constant;
import net.galvin.hadoop.comm.utils.Logging;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * Created by Administrator on 2017/8/24.
 */
public class Demo {

    public static void main(String[] args) {
        Logging.info("starting to process");
        Configuration configuration = new Configuration();
        FileSystem fileSystem = null;
        FSDataOutputStream fsDataOutputStream = null;
        FSDataInputStream fsDataInputStream = null;
        InputStream inputStream = null;
        try {
            fileSystem = FileSystem.get(URI.create(Constant.HDFS_URL), configuration);

            //读取目录/temp下有哪些文件合目录
            FileStatus[] fileStatusArr = fileSystem.listStatus(new Path("/temp"));
            for(FileStatus fileStatus : fileStatusArr){
                Logging.info(fileStatus.toString());
            }

            //创建一个文件test.txt，并且写入内容
            fsDataOutputStream = fileSystem.create(new Path("/temp/test.txt"));
            for(int i=0; i<1000; i++){
                fsDataOutputStream.write(("Hello World! This is test. This is line "+(i+1)).getBytes());
            }
            fsDataOutputStream.flush();

            //读取文件test.txt，打印出来
            inputStream = fileSystem.open(new Path("/temp/test.txt"));
            fsDataInputStream = new FSDataInputStream(inputStream);
            byte[] bytes = new byte[1024*1024];
            fsDataInputStream.read(bytes);
            String content = new String(bytes);
            Logging.info(content);
        } catch (IOException e) {
            Logging.info("exception occur");
            Logging.error(e.getMessage());
        }finally {
            if(fsDataOutputStream != null){
                try {
                    fsDataOutputStream.close();
                } catch (IOException e) {
                    Logging.error(e.getMessage());
                }
            }

            if(inputStream != null){
                try {
                    inputStream.close();
                } catch (IOException e) {
                    Logging.error(e.getMessage());
                }
            }

            if(fsDataInputStream != null){
                try {
                    fsDataInputStream.close();
                } catch (IOException e) {
                    Logging.error(e.getMessage());
                }
            }

            if(fileSystem != null){
                try {
                    fileSystem.close();
                } catch (IOException e) {
                    Logging.error(e.getMessage());
                }
            }
        }
        Logging.info("ending to process");
    }

}
