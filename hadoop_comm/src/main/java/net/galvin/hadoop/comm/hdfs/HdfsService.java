package net.galvin.hadoop.comm.hdfs;

import org.apache.hadoop.fs.FileStatus;

import java.util.List;

/**
 * Created by Administrator on 2017/8/24.
 */
public interface HdfsService {

    List<FileStatus> listDir(String dirName);

    boolean createDir(String DirName);

    boolean createFile(String fileName);

    boolean append(String fileName, String content);

    String cat(String fileName);

}
