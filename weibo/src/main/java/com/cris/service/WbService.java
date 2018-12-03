package com.cris.service;

import com.cris.dao.WbContentDao;
import com.cris.dao.WbHbaseDao;
import com.cris.dao.WbInboxDao;
import com.cris.dao.WbRelationDao;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.util.List;

/**
 * 业务逻辑层
 *
 * @author cris
 * @version 1.0
 **/
@SuppressWarnings("JavaDoc")
@Data
@AllArgsConstructor
@Accessors(chain = true)
public class WbService {

    private final WbContentDao wbContentDao = new WbContentDao();
    private final WbRelationDao wbRelationDao = new WbRelationDao();
    private final WbInboxDao wbInboxDao = new WbInboxDao();


    public void start() {
        WbHbaseDao.getConnection();
    }

    public void end() throws IOException {
        WbHbaseDao.closeConnection();
    }

    /**
     * star 发布微博 ok
     *
     * @param star    明星
     * @param content 发布的微博内容
     */
    public void publishWb(String star, String content) throws IOException {
        Connection connection = WbHbaseDao.getConnection();
        // 微博内容表增加一条记录 ok
        byte[] rowkey = wbContentDao.addContent(connection, star, content);
        // 获取该 star 的所有粉丝 fans ok
        List<String> fans = wbRelationDao.getAllFans(connection, star);
        // 向这些粉丝的收件箱发送微博消息 ok
        wbInboxDao.receiveMessage(connection, star, rowkey, fans);
    }

    /**
     * 用户关注 star
     *
     * @param star 名字
     * @param fan  粉丝
     * @throws IOException
     */
    public void attend(String star, String fan) throws IOException {
        Connection connection = WbHbaseDao.getConnection();
        // star 的粉丝列表更新 ok
        wbRelationDao.addFans(connection, star, fan);
        // fan 的 attend 列表更新 ok
        wbRelationDao.addAttend(connection, star, fan);
        // fan 的收件箱收到 star 最近更新的 5 条微博
        wbInboxDao.flush(connection, star, fan);
    }

    /**
     * 用户取消关注
     *
     * @param star
     * @param fan
     */
    public void removeAttend(String star, String fan) throws IOException {
        Connection connection = WbHbaseDao.getConnection();
        wbRelationDao.removeFan(connection, star, fan);
        wbRelationDao.removeAttend(connection, star, fan);
        wbInboxDao.removeFlush(connection, star, fan);
    }

}
