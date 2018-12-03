package com.cris.mr2;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 定义的 Mapper 类，从 HDFS 上读取文件然后转换为 Put
 *
 * @author cris
 * @version 1.0
 **/
public class MyMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

    /**
     * 将得到的每行数据转换为 put
     *
     * @param key     每行文本的偏移量
     * @param value   每行文本
     * @param context 上下文
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        // 得到 rowkey
        byte[] rowkey = Bytes.toBytes(split[0]);

        // 组装 put
        Put put = new Put(rowkey);
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(split[1]));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("color"), Bytes.toBytes(split[2]));
        context.write(new ImmutableBytesWritable(rowkey), put);
    }
}
