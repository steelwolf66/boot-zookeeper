package com.wolf.base.controller;

import com.wolf.base.service.MyZkConnect;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

@RestController
public class ZkController {
    Logger logger = LoggerFactory.getLogger(ZkController.class);

    @PostMapping("/zk")
    public void zk() throws IOException, InterruptedException, KeeperException {
        //建立连接
        ZooKeeper zk = MyZkConnect.connect();
        //zk.close();//关闭后不支持重连
        logger.info("zk 状态：" + zk.getState());

        /**恢复会话连接**/
        //long sessionId = zk.getSessionId();
        //byte[] sessionPasswd = zk.getSessionPasswd();
        //zk2会话重连后，zk会话将失效，不再支持做增删改查操作。
        //ZooKeeper zk2 = reconnect(sessionId, sessionPasswd);

        /**创建节点**/
        MyZkConnect.create(zk, "/myzk", "myzk");
    }

    @GetMapping("/zk")
    public String getData() throws IOException, InterruptedException, KeeperException {
        //建立连接
        ZooKeeper zk = MyZkConnect.connect();
        //zk.close();//关闭后不支持重连
        logger.info("zk 状态：" + zk.getState());

        /**恢复会话连接**/
        //long sessionId = zk.getSessionId();
        //byte[] sessionPasswd = zk.getSessionPasswd();
        //zk2会话重连后，zk会话将失效，不再支持做增删改查操作。
        //ZooKeeper zk2 = reconnect(sessionId, sessionPasswd);

        /**查询节点**/
        String data = MyZkConnect.queryData(zk, "/myzk");
        return data;
    }

    @DeleteMapping("/zk")
    public void deleteData() throws IOException, InterruptedException, KeeperException {
        //建立连接
        ZooKeeper zk = MyZkConnect.connect();
        //zk.close();//关闭后不支持重连
        logger.info("zk 状态：" + zk.getState());

        /**恢复会话连接**/
        //long sessionId = zk.getSessionId();
        //byte[] sessionPasswd = zk.getSessionPasswd();
        //zk2会话重连后，zk会话将失效，不再支持做增删改查操作。
        //ZooKeeper zk2 = reconnect(sessionId, sessionPasswd);

        /**查询节点**/
        MyZkConnect.delete(zk, "/myzk");
    }

    @GetMapping("/health")
    public String isSurvive() {
        return "health";
    }
}
