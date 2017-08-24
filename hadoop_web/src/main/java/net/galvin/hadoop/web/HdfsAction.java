package net.galvin.hadoop.web;

import net.galvin.hadoop.comm.hdfs.HdfsService;
import org.apache.hadoop.fs.FileStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
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
    public Object listDir(HttpServletRequest request){
        String dirName = request.getParameter("dirName");
        List<FileStatus> list = hdfsService.listDir(dirName);
        return list;
    }

}
