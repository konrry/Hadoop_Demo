package net.galvin.hadoop.web;

import net.galvin.hadoop.comm.hdfs.HdfsService;
import net.galvin.hadoop.comm.utils.Logging;
import org.apache.hadoop.fs.FileStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2017/8/24.
 */
@Controller
@RequestMapping("/hdfs")
public class HdfsAction {

    @Autowired
    private HdfsService hdfsService;


    @RequestMapping("/listDir")
    @ResponseBody
    public Object listDir(HttpServletRequest request, HttpServletResponse response){
        String dirName = request.getParameter("dirName");
        List<FileStatus> fileStatusList = hdfsService.listDir(dirName);
        List<String> fileList = new ArrayList<String>();
        for(FileStatus fileStatus : fileStatusList){
            fileList.add(fileStatus.toString());
        }
        return fileList;
    }

}
