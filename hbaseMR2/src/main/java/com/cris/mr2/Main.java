package com.cris.mr2;

import org.apache.hadoop.util.ToolRunner;

/**
 * 主程序入口
 *
 * @author cris
 * @version 1.0
 **/
public class Main {
    public static void main(String[] args) {
        try {
            ToolRunner.run(new MyTool(),args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
