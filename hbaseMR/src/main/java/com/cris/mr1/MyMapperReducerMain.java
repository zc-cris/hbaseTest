package com.cris.mr1;

import org.apache.hadoop.util.ToolRunner;

/**
 * 主方法运行我们的 MapReduce job
 *
 * @author cris
 * @version 1.0
 **/
public class MyMapperReducerMain {
    public static void main(String[] args) {
        try {
            ToolRunner.run(new MyTool(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
