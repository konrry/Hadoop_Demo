package net.galvin.hadoop.comm.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Administrator on 2017/8/24.
 */
public class Logging {

//    private final static Logger LOGGER = LoggerFactory.getLogger(Logging.class);

    public final static void info(String message){
//        LOGGER.info(message);
        System.out.println(message);
    }

    public final static void error(String message){
//        LOGGER.error(message);
        System.out.println(message);
    }

}
