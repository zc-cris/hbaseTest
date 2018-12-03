package com.cris.controller;

import com.cris.service.WbService;

import java.io.IOException;

/**
 * 控制层
 *
 * @author cris
 * @version 1.0
 **/
@SuppressWarnings("JavaDoc")
public class WbController {


    private WbService wbService = new WbService();

    public WbService getWbService() {
        return wbService;
    }

    /**
     * star 发布微博
     *
     * @param star
     * @param content
     * @throws IOException
     */
    public void publishWb(String star, String content) throws IOException {
        wbService.publishWb(star, content);
    }

    /**
     * 用户关注 star
     *
     * @param star
     * @param fan
     * @throws IOException
     */
    public void attend(String star, String fan) throws IOException {
        wbService.attend(star, fan);
    }

    /**
     * 用户取消关注 star
     *
     * @param star
     * @param fan
     * @throws IOException
     */
    public void removeAttend(String star, String fan) throws IOException {
        wbService.removeAttend(star, fan);
    }

}
