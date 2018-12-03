package com.cris;

import com.cris.controller.WbController;
import com.cris.dao.WbHbaseDao;
import com.cris.dao.WbRelationDao;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * 测试代码
 *
 * @author cris
 * @version 1.0
 **/
@SuppressWarnings("JavaDoc")
public class WbTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(String.valueOf(WbTest.class));

    private WbController wbController = new WbController();

    @Before
    public void testStart() {
        wbController.getWbService().start();
    }

    @After
    public void testEnd() throws IOException {
        wbController.getWbService().end();
    }

    /**
     * 测试 ok
     *
     * @throws IOException
     */
    @Test
    public void testInitNameSpace() throws IOException {
        WbHbaseDao.initNameSpace(WbHbaseDao.WB_NAMESPACENAME);
        LOGGER.warn("命名空间创建成功！");
    }

    /**
     * 测试 ok
     *
     * @throws IOException
     */
    @Test
    public void testRemoveNameSpace() throws IOException {
        WbHbaseDao.removeNameSpace(WbHbaseDao.WB_NAMESPACENAME);
        LOGGER.warn("命名空间删除成功");
    }


    /**
     * 初始化项目所需要的三张表：微博内容表，微博用户关系表，微博用户收件箱表
     * 测试 ok
     *
     * @throws IOException
     */
    @Test
    public void testInitTables() throws IOException {
        WbHbaseDao.initTables(WbHbaseDao.WB_CONTENT_TABLENAME, "info");
        WbHbaseDao.initTables(WbHbaseDao.WB_RELATION_TABLENAME, WbRelationDao.COLUMN_FAMILY_ATTENDS, WbRelationDao.COLUMN_FAMILY_FANS);
        WbHbaseDao.initTables(WbHbaseDao.WB_INBOX_TABLENAME, 5, "info");
        LOGGER.warn("创建表成功！");
    }

    /**
     * 删除 HBase 表（测试ok,记得带上命名空间）
     *
     * @throws IOException
     */
    @Test
    public void testDeleteTables() throws IOException {
        WbHbaseDao.removeTable("cris:wb_content");
        WbHbaseDao.removeTable("cris:wb_relation");
        WbHbaseDao.removeTable("cris:wb_inbox");
        LOGGER.warn("删除表成功！");
    }

    /**
     * 测试 star 的粉丝添加 ok
     *
     * @throws IOException
     */
    @Test
    public void testAddFans() throws IOException {
        wbController.getWbService().getWbRelationDao().addFans(WbHbaseDao.getConnection(), "james", "cris");
        wbController.getWbService().getWbRelationDao().addFans(WbHbaseDao.getConnection(), "james", "curry");
        wbController.getWbService().getWbRelationDao().addFans(WbHbaseDao.getConnection(), "james", "harden");
        LOGGER.warn("star 的粉丝列表添加成功！");
    }

    /**
     * 测试粉丝的 attend 添加 ok
     *
     * @throws IOException
     */
    @Test
    public void testAddAttend() throws IOException {
        wbController.getWbService().getWbRelationDao().addAttend(WbHbaseDao.getConnection(), "james", "cris");
        wbController.getWbService().getWbRelationDao().addAttend(WbHbaseDao.getConnection(), "james", "curry");
        wbController.getWbService().getWbRelationDao().addAttend(WbHbaseDao.getConnection(), "james", "harden");
        LOGGER.warn("用户的关注列表添加成功");
    }

    /**
     * 获取指定 star 的所有粉丝列表，测试 ok
     *
     * @throws IOException
     */
    @Test
    public void testGetAllFans() throws IOException {
        List<String> allFans = wbController.getWbService().getWbRelationDao().getAllFans(WbHbaseDao.getConnection(), "james");
        allFans.forEach(System.out::println);

//        List<String> allFans = wbController.getWbService().getWbRelationDao().getAllFans(WbHbaseDao.getConnection(), "curry");
//        allFans.forEach(System.out::println);
    }

    /**
     * 整合功能测试（发布微博） ok
     *
     * @throws IOException
     */
    @Test
    public void testPublishWb() throws IOException {
        wbController.publishWb("james", "i like k");
        LOGGER.warn("发布成功！");
    }

    /**
     * 整合功能测试（关注 star） ok
     *
     * @throws IOException
     */
    @Test
    public void testAttend() throws IOException {
        wbController.attend("james", "durant");
        LOGGER.warn("关注成功！");
    }

    /**
     * 整合功能测试（取消关注 star）ok
     *
     * @throws IOException
     */
    @Test
    public void testRemoveAttend() throws IOException {
        wbController.removeAttend("james", "durant");
        LOGGER.warn("取消关注成功！");
    }


}
