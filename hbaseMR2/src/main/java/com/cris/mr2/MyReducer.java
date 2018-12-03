package com.cris.mr2;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

/**
 * 自定义的 Reducer 类，用于将读取的每行 Put 数据输出到 HBase 表中
 *
 * @author cris
 * @version 1.0
 **/
public class MyReducer extends TableReducer<ImmutableBytesWritable, Put, NullWritable> {

    /**
     * 将已经按照 rowkey 分好组的数据写出去
     *
     * @param key     rowkey
     * @param values  每个 rowkey 对应的一组 cell 数据
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
        for (Put value : values) {
            context.write(NullWritable.get(), value);
        }
    }
}
