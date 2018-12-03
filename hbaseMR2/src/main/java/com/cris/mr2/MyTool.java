package com.cris.mr2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;

/**
 * 自定义的 MapReduce，用于将 HDFS 上的 tsv 文件数据导入到 HBase 表中
 *
 * @author cris
 * @version 1.0
 **/
public class MyTool implements Tool {

    public int run(String[] strings) throws Exception {

        Job job = Job.getInstance();
        job.setJarByClass(MyTool.class);

        /*指定要导入的数据在 HDFS 上的路径*/
        FileInputFormat.addInputPath(job, new Path("hdfs://hadoop101:9000/input_fruit/fruit1.tsv"));

        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        TableMapReduceUtil.initTableReducerJob("fruit_mr", MyReducer.class, job);

        boolean flag = job.waitForCompletion(true);

        return flag ? JobStatus.State.SUCCEEDED.getValue() : JobStatus.State.FAILED.getValue();
    }

    public void setConf(Configuration configuration) {

    }

    public Configuration getConf() {
        return null;
    }
}
