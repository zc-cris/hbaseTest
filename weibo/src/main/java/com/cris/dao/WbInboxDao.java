package com.cris.dao;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * 和 inbox 包交互类
 *
 * @author cris
 * @version 1.0
 **/
@SuppressWarnings("JavaDoc")
public class WbInboxDao {

    private static final String COLUMN_FAMILY_INFO = "info";
    private static final int DEFAULT_MESSAGE_COUNT = 5;

    /**
     * 通知所有粉丝的收件箱查看关注的 star 新发布的微博
     *
     * @param connection 连接
     * @param star       star 名字
     * @param rowkey     star 新发布的微博 rowkey
     * @param fans       粉丝列表们
     */
    public void receiveMessage(Connection connection, String star, byte[] rowkey, List<String> fans) throws IOException {
        Table table = connection.getTable(TableName.valueOf(WbHbaseDao.WB_INBOX_TABLENAME));
        Put put;
        for (String fan : fans) {
            put = new Put(Bytes.toBytes(fan));
            put.addColumn(Bytes.toBytes(COLUMN_FAMILY_INFO), Bytes.toBytes(star), rowkey);
            table.put(put);
        }
        WbHbaseDao.closeTable(table);
    }


    /**
     * 用户关注某个用户后，收件箱自动获取该 star 最新的 5 条微博数据
     *
     * @param connection 连接
     * @param star       明星名字
     * @param fan        粉丝名字
     */
    public void flush(Connection connection, String star, String fan) throws IOException {
        Table table = connection.getTable(TableName.valueOf(WbHbaseDao.WB_CONTENT_TABLENAME));
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(star));
        /*这里使用倒数第三大的 | 符号来组装 rowkey，需要根据开始的 rowkey 和 结束的 rowkey 得到该明星发布的最新的 5 条微博*/
        scan.setStopRow(Bytes.toBytes(star + "|"));
        ResultScanner resultScanner = table.getScanner(scan);
        List<Put> puts = new ArrayList<>(DEFAULT_MESSAGE_COUNT);
        /*最好在添加每条微博数据的时候自定义时间戳，否则极易因为程序执行太快导致时间戳相同而无法插入多条数据*/
        int i = 0;
        /*这里得到的每一个 result 其实就是每一个 rowkey 对应的数据*/
        for (Result result : resultScanner) {
            String message = new String(result.getRow(), StandardCharsets.UTF_8);
            Put put = new Put(Bytes.toBytes(fan));
            put.addColumn(Bytes.toBytes(COLUMN_FAMILY_INFO), Bytes.toBytes(star),
                    System.currentTimeMillis() + (++i), Bytes.toBytes(message));
            puts.add(put);
            if (puts.size() == 5) {
                break;
            }
        }

        table = connection.getTable(TableName.valueOf(WbHbaseDao.WB_INBOX_TABLENAME));
        table.put(puts);
        WbHbaseDao.closeTable(table);
    }


    /**
     * 粉丝取消关注后自动移除该粉丝收件箱，关于取消关注的 star 的所有消息
     *
     * @param connection 连接
     * @param star       明星名字
     * @param fan        粉丝名字
     * @throws IOException
     */
    public void removeFlush(Connection connection, String star, String fan) throws IOException {
        Table table = connection.getTable(TableName.valueOf(WbHbaseDao.WB_INBOX_TABLENAME));
        Delete delete = new Delete(Bytes.toBytes(fan));
        delete.addColumn(Bytes.toBytes(COLUMN_FAMILY_INFO), Bytes.toBytes(star));
        table.delete(delete);
        WbHbaseDao.closeTable(table);
    }
}
