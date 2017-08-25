package net.galvin.hadoop.web;

import com.alibaba.fastjson.JSON;
import com.google.gson.JsonArray;
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
    public List<String> listDir(HttpServletRequest request, HttpServletResponse response){
        String dirName = request.getParameter("dirName");
        List<String> fileList = new ArrayList<String>();
        List<FileStatus> fileStatusList = hdfsService.listDir(dirName);
        for(FileStatus fileStatus : fileStatusList){
            fileList.add(fileStatus.toString());
        }
        return fileList;
    }


    @RequestMapping("/cat")
    @ResponseBody
    public List<String> cat(HttpServletRequest request, HttpServletResponse response){
        String fileName = request.getParameter("fileName");
        List<String> contentList = hdfsService.cat(fileName);
        return contentList;
    }

}
