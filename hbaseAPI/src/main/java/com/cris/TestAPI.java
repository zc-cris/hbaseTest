package com.cris;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.HBaseConfTool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * 测试 HBase 的常用 API
 *
 * @author cris
 * @version 1.0
 **/
@SuppressWarnings("JavaDoc")
public class TestAPI {

    private static final Logger LOGGER = LoggerFactory.getLogger(String.valueOf(TestAPI.class));
    private Connection connection;
    private final TableName tableName = TableName.valueOf("stu");
    private Admin admin;
    private final Configuration configuration = HBaseConfiguration.create();
    private Table table;


    /**
     * 创建连接对象
     */
    @Before
    public void testGetConnection() {
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 测试表是否存在
     */
    @Test
    public void testTableExists() {
        try {
            boolean tableExists = admin.tableExists(tableName);
            LOGGER.info("判断表是否存在" + tableExists);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 关闭连接和远程服务器映射对象
     *
     * @throws IOException
     */
    @After
    public void close() throws IOException {
        if (admin != null) {
            admin.close();
        }
        if (connection != null) {
            connection.close();
        }
        if (table != null) {
            table.close();
        }
    }

    /**
     * 测试表的删除和创建
     */
    @Test
    public void testCreateAndDeleteTable() {
        try {

            boolean tableExists = admin.tableExists(tableName);

            if (tableExists) {
                // 删除表之前先要设置表为不可用状态
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
                LOGGER.warn("删除表成功！");
            } else {
                // 根据表名创建表描述器
                HTableDescriptor descriptor = new HTableDescriptor(tableName);
                // 为表描述器创建列族描述器
                List<String> list = Arrays.asList("info");
                for (String s : list) {
                    descriptor.addFamily(new HColumnDescriptor(s));
                }
                admin.createTable(descriptor);
                LOGGER.warn("创建表成功！");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 为表增加数据
     */
    @Test
    public void testAddData() {
        try {
            table = connection.getTable(tableName);
            Put put = new Put(Bytes.toBytes("1001"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("cris"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("23"));
            table.put(put);
            LOGGER.warn("数据创建成功！");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除表中数据
     */
    @Test
    public void testDeleteData() {
        try {
            table = connection.getTable(tableName);
            List<Delete> list = new ArrayList<Delete>(1);
            list.add(new Delete(Bytes.toBytes("1001")));
            table.delete(list);
            LOGGER.warn("删除数据成功！");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 根据需要获取多条数据
     */
    @Test
    public void testGetAllData() {
        try {
            // 获取表的映射对象
            table = connection.getTable(tableName);
            // 可以定制 Scan 对数据进行筛选
            ResultScanner resultScanner = table.getScanner(new Scan());
            // 对得到的数据集遍历
            for (Result result : resultScanner) {
                /*数据集里的每组 rowkey 都有多个单元格数据，根据列族的列信息个数决定；每个单元格信息都包含 rowkey，cf 以及对应的列信息*/
                Cell[] cells = result.rawCells();
                for (Cell cell : cells) {
                    printRowInfo(cell);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 根据 rowkey 获取每组单元格数据
     */
    @Test
    public void testGetDataByRowKey() {
        try {
            table = connection.getTable(tableName);
            Get get = new Get(Bytes.toBytes("1001"));

///           get.setMaxVersions();
//            get.setTimeStamp();

            /*根据 get 封装的信息得到 rowkey 对应的所有单元格信息*/
            Result result = table.get(get);
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                printRowInfo(cell);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 打印每组 rowkey 数据的信息（每行数据可能有多个单元格数据，根据列族的列信息划分）
     *
     * @param cell 单元格信息
     */
    private void printRowInfo(Cell cell) {
        String rowKey = Bytes.toString(CellUtil.cloneRow(cell));
        String cf = Bytes.toString(CellUtil.cloneFamily(cell));
        String columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
        String columnValue = Bytes.toString(CellUtil.cloneValue(cell));
        LOGGER.warn(String.format("rowKey = %s, columnFamily=%s, columnName=%s, columnValue=%s", rowKey,
                cf, columnName, columnValue));
    }


    /**
     * 根据指定的 rowkey，列族和列信息获取对应的数据
     *
     * @throws IOException
     */
    @Test
    public void testGetDataByColumn() throws IOException {
        table = connection.getTable(tableName);
        Get get = new Get(Bytes.toBytes("1001"));
        get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));
        Result result = table.get(get);
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            printRowInfo(cell);
        }
    }

}
