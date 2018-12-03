package com.cris.dao;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 微博内容表交互类
 *
 * @author cris
 * @version 1.0
 **/
public class WbContentDao {

    public static final String COLUMN_FAMILY_NAME = "info";
    public static final String COLUMN_NAME = "content";

    /**
     * 微博内容表增加一条记录
     *
     * @param connection
     * @param star
     * @param content
     */
    public byte[] addContent(Connection connection, String star, String content) throws IOException {
        Table table = connection.getTable(TableName.valueOf(WbHbaseDao.WB_CONTENT_TABLENAME));

        /*HBase 表存储数据默认按照 rowkey 的字符串大小比较排序，即字符串小的 rowkey 排在上面
         * 为了取出 star 最新发布的微博内容，这里需要使用 Long 的最大值减去时间戳得到 rowkey：
         * 微博发布时间越新，时间戳越大，Long 的最大值减去时间戳得到的 rowkey 就越小，在 Hbase 表排序越往上*/
        byte[] rowkey = Bytes.toBytes(star + (Long.MAX_VALUE - System.currentTimeMillis()));
        Put put = new Put(rowkey);
        put.addColumn(Bytes.toBytes(COLUMN_FAMILY_NAME), Bytes.toBytes(COLUMN_NAME), Bytes.toBytes(content));
        table.put(put);
        WbHbaseDao.closeTable(table);
        return rowkey;
    }
}
