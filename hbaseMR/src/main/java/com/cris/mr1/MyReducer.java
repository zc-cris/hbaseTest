package com.cris.mr1;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

/**
 * 自定义的 Reducer 类，对按照 rowkey 分组的数据中的每个单元格数据又一条一条写出去
 *
 * @author cris
 * @version 1.0
 **/
public class MyReducer extends TableReducer<ImmutableBytesWritable, Put, NullWritable> {

    /**
     * @param key 每个 rowkey
     * @param values 经过分组，每个 rowkey 都有对应的一组单元格信息
     * @param context 上下文对象
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context) throws IOException, InterruptedException {

        for (Put value : values) {
            context.write(NullWritable.get(),value);
        }
    }
}
