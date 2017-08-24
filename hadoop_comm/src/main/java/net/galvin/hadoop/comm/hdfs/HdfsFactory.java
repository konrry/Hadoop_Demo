package net.galvin.hadoop.comm.hdfs;

import net.galvin.hadoop.comm.utils.Constant;
import net.galvin.hadoop.comm.utils.Logging;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.net.URI;

/**
 * Created by Administrator on 2017/8/24.
 */
public class HdfsFactory {

    private static FileSystem fileSystem = null;

    public static FileSystem get(){
        if(fileSystem == null){
            synchronized (HdfsFactory.class){
                if(fileSystem == null){
                    Configuration configuration = new Configuration();
                    configuration.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem");
                    try {
                        fileSystem = FileSystem.get(URI.create(Constant.HDFS_URL), configuration);
                    } catch (Exception e) {
                        Logging.error(e.getMessage());
                    }
                }
            }
        }
        return fileSystem;
    }

}
