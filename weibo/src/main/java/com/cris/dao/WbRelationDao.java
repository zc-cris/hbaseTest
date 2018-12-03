package com.cris.dao;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * 用来表示用户之间关系的数据表，这里使用用户名字作为 rowkey，使用用户名字作为 fans 或者 attends 的列
 * 实际开发中应该使用用户 id，这里为了方便展示，故使用用户姓名
 *
 * @author cris
 * @version 1.0
 **/
@SuppressWarnings("JavaDoc")
public class WbRelationDao {

    public static final String COLUMN_FAMILY_FANS = "fans";
    public static final String COLUMN_FAMILY_ATTENDS = "attends";


    /**
     * 根据 star 的名字获取他的所有粉丝名字
     *
     * @param connection
     * @param star
     * @return 粉丝列表
     * @throws IOException
     */
    public List<String> getAllFans(Connection connection, String star) throws IOException {
        Table table = connection.getTable(TableName.valueOf(WbHbaseDao.WB_RELATION_TABLENAME));
        // star 名字作为 rowkey ，搜索对应的所有粉丝列表
        Get get = new Get(Bytes.toBytes(star));
        get.addFamily(Bytes.toBytes(COLUMN_FAMILY_FANS));
        //Result result = table.get(get);

        //这里使用 Optional 包装一下，空指针直接报错
        Optional<Result> result = Optional.of(table.get(get));
        /*如果 star 没有粉丝列表也不会报错，返回的是一个没有数据的 cell[]*/
        Cell[] cells = result.get().rawCells();
        List<String> list = new ArrayList<>();
        for (Cell cell : cells) {
            list.add(new String(CellUtil.cloneQualifier(cell), StandardCharsets.UTF_8));
        }

        return list;
    }

    /**
     * 用户的 fans 列组添加粉丝
     *
     * @param connection
     * @param star       明星
     * @param fan        粉丝
     * @throws IOException
     */
    public void addFans(Connection connection, String star, String fan) throws IOException {
        addFansOrStar(connection, fan, star, COLUMN_FAMILY_FANS);
    }

    /**
     * 用户的 attends 列组添加 star
     *
     * @param connection
     * @param star
     * @param fan
     * @throws IOException
     */
    public void addAttend(Connection connection, String star, String fan) throws IOException {
        addFansOrStar(connection, star, fan, COLUMN_FAMILY_ATTENDS);
    }

    /**
     * 实际的添加 fan 或者 star 的方法
     *
     * @param connection   连接
     * @param column       star 或 fan 的名字
     * @param rowkey       用户名字
     * @param columnFamily 列族（每个人的 fans 列族或者 attends 列族）
     * @throws IOException
     */
    private void addFansOrStar(Connection connection, String column, String rowkey, String columnFamily) throws IOException {
        Table table = connection.getTable(TableName.valueOf(WbHbaseDao.WB_RELATION_TABLENAME));
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), null);
        table.put(put);
        WbHbaseDao.closeTable(table);
    }

    /**
     * star 的粉丝列表移除粉丝
     *
     * @param connection
     * @param star
     * @param fan
     * @throws IOException
     */
    public void removeFan(Connection connection, String star, String fan) throws IOException {
        removeFansOrStar(connection, fan, star, COLUMN_FAMILY_FANS);
    }

    /**
     * fan 的 attend 列表移除 star
     *
     * @param connection
     * @param star
     * @param fan
     * @throws IOException
     */
    public void removeAttend(Connection connection, String star, String fan) throws IOException {
        removeFansOrStar(connection, star, fan, COLUMN_FAMILY_ATTENDS);
    }

    /**
     * 实际的移除 fan 或者 star 的方法
     *
     * @param connection   连接
     * @param column       star 或 fan 的名字
     * @param rowkey       用户名字
     * @param columnFamily 列族（每个人的 fans 列族或者 attends 列族）
     * @throws IOException
     */
    private void removeFansOrStar(Connection connection, String column, String rowkey, String columnFamily) throws IOException {
        Table table = connection.getTable(TableName.valueOf(WbHbaseDao.WB_RELATION_TABLENAME));
        Delete delete = new Delete(Bytes.toBytes(rowkey));
        delete.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
        table.delete(delete);
        WbHbaseDao.closeTable(table);
    }
}
