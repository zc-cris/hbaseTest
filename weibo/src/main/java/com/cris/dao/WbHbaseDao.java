package com.cris.dao;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.Optional;

/**
 * 和 HBase 交互的工具类
 *
 * @author cris
 * @version 1.0
 **/
@SuppressWarnings("SpellCheckingInspection")
public class WbHbaseDao {

    /**
     * 协助当前线程存储数据到一个map中，key 就是当前 ThreadLocal 对象，值就是要存储的数据
     **/
    private static final ThreadLocal<Connection> THREAD_LOCAL = new ThreadLocal<>();
    /**
     * 默认列数据的最大版本数为1，HBase 默认就是1
     **/
    private static final int DEFAULT_MAX_VERSION = 1;

    public static final String WB_CONTENT_TABLENAME = "cris:wb_content";
    public static final String WB_RELATION_TABLENAME = "cris:wb_relation";
    public static final String WB_INBOX_TABLENAME = "cris:wb_inbox";
    public static final String WB_NAMESPACENAME = "cris";


    /**
     * 获取连接，结合 Optional 可以确保得到的连接一定不为空！！！如果连接有问题就直接报错了～
     *
     * @return 连接
     */
    public static Connection getConnection() {
        Optional<Connection> optionalConnection = Optional.ofNullable(THREAD_LOCAL.get());

        /*如果 optionalConnection 有值，那么返回容器中的值，否则为当前线程的 ThreadLocalMap 存储一个 connection*/
        return optionalConnection.orElseGet(() -> {
            try {
                /*创建 Connection 过程中报错就直接抛出空指针异常，确保每个当前线程都应该有 connection
                 * 并且需要保证从当前线程得到的 onnection 一定是非空值*/
                Connection conn = ConnectionFactory.createConnection();
                THREAD_LOCAL.set(conn);
                return conn;
            } catch (IOException e) {
                e.printStackTrace();
                throw new NullPointerException();
            }
        });

    }

    /**
     * 关闭连接
     */
    public static void closeConnection() throws IOException {
        Connection connection = THREAD_LOCAL.get();
        closeAdmin(connection.getAdmin());
        connection.close();
        /*注意：一定要移除线程中绑定的 Connection 对象*/
        THREAD_LOCAL.remove();
    }

    public static void closeAdmin(Admin admin) throws IOException {
        if (admin != null) {
            admin.close();
        }
    }

    public static void closeTable(Table table) throws IOException {
        if (table != null) {
            table.close();
        }
    }

    /**
     * 初始化命名空间
     *
     * @param nameSpaceName 命名空间名字
     * @throws IOException
     */
    public static void initNameSpace(String nameSpaceName) throws IOException {
        Connection connection = getConnection();
        Admin admin = connection.getAdmin();
        // 先要判断 nameSpace 是否存在，如果不存在那么创建
        if (!existNameSpace(nameSpaceName)) {
            NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpaceName).build();
            admin.createNamespace(namespaceDescriptor);
        }
    }

    /**
     * 判断命名空间是否存在
     *
     * @param nameSpace 命名空间名字
     * @return 命名空间存在返回 true
     * @throws IOException
     */
    public static boolean existNameSpace(String nameSpace) throws IOException {
        Connection connection = getConnection();
        Admin admin = connection.getAdmin();
        try {
            /*如果命名空间不存在将会抛出 NamespaceNotFoundException */
            NamespaceDescriptor namespaceDescriptor = admin.getNamespaceDescriptor(nameSpace);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

    }

    /**
     * 移除命名空间
     *
     * @param nameSpaceName 命名空间名字
     * @throws IOException
     */
    public static void removeNameSpace(String nameSpaceName) throws IOException {
        Connection connection = getConnection();
        Admin admin = connection.getAdmin();
        // 先要判断 nameSpace 是否存在，如果存在那么删除
        boolean existNameSpace = existNameSpace(nameSpaceName);
        if (existNameSpace) {
            admin.deleteNamespace(nameSpaceName);
        }
    }

    /**
     * 移除 HBase 表
     *
     * @param tableName 表名
     * @throws IOException
     */
    public static void removeTable(String tableName) throws IOException {
        Connection connection = getConnection();
        Admin admin = connection.getAdmin();
        TableName name = TableName.valueOf(tableName);
        boolean tableExists = admin.tableExists(name);
        if (tableExists) {
            admin.disableTable(name);
            admin.deleteTable(name);
        }
    }


    /**
     * 初始化项目所需要的 HBase 表
     *
     * @param tableName
     * @param maxVersion
     * @param columnNames
     * @throws IOException
     */
    public static void initTables(String tableName, int maxVersion, String... columnNames) throws IOException {
        Connection connection = getConnection();
        Admin admin = connection.getAdmin();
        createTable(admin, tableName, maxVersion, columnNames);
    }

    /**
     * 初始化项目所需要的 HBase 表
     *
     * @param tableName
     * @param columnNames
     * @throws IOException
     */
    public static void initTables(String tableName, String... columnNames) throws IOException {
        initTables(tableName, DEFAULT_MAX_VERSION, columnNames);
    }

    /**
     * 实际的创建 HBase 表的方法
     *
     * @param admin       HBase 集群的主机（HMaster 节点），负责协调 HBase 集群
     * @param tableName   表名
     * @param columnNames 列名（可能多个）
     * @param maxVersion  可手动设置最大版本号
     * @throws IOException
     */
    public static void createTable(Admin admin, String tableName, int maxVersion, String... columnNames) throws IOException {
        TableName name = TableName.valueOf(tableName);
        boolean tableExists = admin.tableExists(name);
        if (!tableExists) {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(name);
            for (String columnName : columnNames) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(columnName);
                hColumnDescriptor.setMaxVersions(maxVersion);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            admin.createTable(hTableDescriptor);
        }
    }
}
