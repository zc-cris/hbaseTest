package com.cris.mr1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.util.Tool;

/**
 * 自定义的 Tool，对一次 MapReduce 任务的封装
 *
 * @author cris
 * @version 1.0
 **/
public class MyTool implements Tool {

    /**
     * 具体的实现逻辑方法
     *
     * @param strings
     * @return
     * @throws Exception
     */
    public int run(String[] strings) throws Exception {

        Job job = Job.getInstance();
        job.setJarByClass(MyTool.class);

        TableMapReduceUtil.initTableMapperJob("fruit",
                new Scan(), MyMapper.class, ImmutableBytesWritable.class, Put.class, job);

        TableMapReduceUtil.initTableReducerJob("fruit_mr", MyReducer.class, job);

        return job.waitForCompletion(true) ? JobStatus.State.SUCCEEDED.getValue() : JobStatus.State.FAILED.getValue();

    }

    public void setConf(Configuration configuration) {

    }

    public Configuration getConf() {
        return null;
    }
}
