package com.cris.mr1;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;

import java.io.IOException;

/**
 * 自定义的 Mapper 类，将 HBase 表中的数据按照 rowkey 分组读取进来，每组数据就是单元格数据的集合
 *
 * @author cris
 * @version 1.0
 **/
public class MyMapper extends TableMapper<ImmutableBytesWritable, Put> {

    /**
     * 这里 HBase 其实已经帮我们简化了 HBase 表和表之间的数据迁移
     *
     * @param key     这里的 key 默认就是对 rowkey 的封装
     * @param value   这里的 value 默认就是每个 rowkey 对应的数据集
     * @param context 上下文对象
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

        Cell[] cells = value.rawCells();
        for (Cell cell : cells) {
            Put put = new Put(key.get());
            put.addColumn(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell), CellUtil.cloneValue(cell));
            context.write(key, put);
        }
    }
}
