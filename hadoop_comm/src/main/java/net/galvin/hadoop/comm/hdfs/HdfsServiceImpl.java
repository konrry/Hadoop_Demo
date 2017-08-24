package net.galvin.hadoop.comm.hdfs;

import net.galvin.hadoop.comm.utils.Logging;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2017/8/24.
 */
@Service
public class HdfsServiceImpl implements HdfsService {

    public List<FileStatus> listDir(String dirName) {
        List<FileStatus> fileStatusList = new ArrayList<FileStatus>();
        try {
            FileStatus[] fileStatusArr = HdfsFactory.get().listStatus(new Path(dirName));
            for(FileStatus fileStatus : fileStatusArr){
                Logging.info(fileStatus.toString());
                fileStatusList.add(fileStatus);
            }
        } catch (IOException e) {
            Logging.error(e.getMessage());
        }
        return fileStatusList;
    }

    public boolean createDir(String DirName) {
        boolean success = false;
        try {
            success = HdfsFactory.get().mkdirs(new Path(DirName));
        } catch (Exception e) {
            Logging.error(e.getMessage());
        }
        return success;
    }

    public Object createFile(String fileName) {
        boolean success = false;
        try {
            HdfsFactory.get().create(new Path(fileName));
            success = true;
        } catch (Exception e) {
            Logging.error(e.getMessage());
        }
        return success;
    }

    public boolean append(String fileName, String content) {
        boolean success = false;
        FSDataOutputStream fsDataOutputStream = null;
        try {
            fsDataOutputStream = HdfsFactory.get().append(new Path(fileName));
            fsDataOutputStream.write((content+"\n").getBytes());
            success = true;
        } catch (Exception e) {
            Logging.error(e.getMessage());
        }finally {
            IOUtils.closeStream(fsDataOutputStream);
        }
        return success;
    }

    public List<String> cat(String fileName) {
        List<String> contentList = new ArrayList<String>();
        FSDataInputStream fsDataInputStream = null;
        try {
            fsDataInputStream = HdfsFactory.get().open(new Path(fileName));
            StringBuilder line = new StringBuilder();
            while (true){
                char c = fsDataInputStream.readChar();
                if('\n' == c){
                    contentList.add(line.toString());
                    line = new StringBuilder();
                }else {
                    line.append(c);
                }
            }
        } catch (Exception e) {
            Logging.error(e.getMessage());
        }finally {
            IOUtils.closeStream(fsDataInputStream);
        }
        return contentList;
    }
}
