package com.ang.provider;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;


/**
 * @author xiaolu.zhang
 * @desc:
 * @date: 2018/3/6 17:36
 */
@Data
@Slf4j
public class MessageWatcher implements Watcher {

    private ZooKeeper zk;

    private String zkServerList;

    private String nodeName = "greyNode";


    /**
     * 创建监听对象并创建zk实例
     **/
    MessageWatcher(String zkServerList, String noteName) {
        try {
            this.nodeName = noteName;
            this.zkServerList = zkServerList;
            zk = new ZooKeeper(zkServerList, 60000, this);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 系统启动后直接拉取最新数据并监控
     **/
    public void init() {
        log.info("系统启动后第一次拉取最新数据");
        watchNode();
    }

    /**
     * 消息处理函数
     **/
    @Override
    public void process(WatchedEvent watchedEvent) {
        if (watchedEvent.getType() == Event.EventType.NodeDataChanged) {
            log.info("收到更新通知，开始更新缓存");
            log.info("更新缓存成功");
            /**zk机制一次出发，所以再次监听**/
            watchNode();
        }

    }


    /**
     * 获取数据并监控节点
     **/
    private void watchNode() {
        zk.getData("/" + nodeName, this, dataCheckCallBack, null);
    }


    /**
     * 获取数据后的回调函数
     **/
    private AsyncCallback.DataCallback dataCheckCallBack = new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                /**链接丢失重新监听**/
                case CONNECTIONLOSS:
                    System.out.println("链接丢失重新监听");
                    watchNode();
                    break;
                /**节点被删除重新创建节点**/
                case NONODE:
                    System.out.println("节点被删除重新创建节点");
                    checkNoteAndWatchNode();
                    break;
                case OK:
                    log.info("正在监听 node {}", nodeName);
                    break;
                default:
                    log.error("some thing went wrong:", KeeperException.create(KeeperException.Code.get(rc), path));
                    break;
            }
        }
    };


    /**
     * 创建标识结点
     **/
    private void checkNoteAndWatchNode() {
        zk.create("/" + nodeName, "-1".getBytes(), OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, nodeCreateCallBack, null);
    }


    /**
     * 创建节点后的回调函数
     **/
    private AsyncCallback.StringCallback nodeCreateCallBack = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                /**链接丢失重试**/
                case CONNECTIONLOSS:
                    checkNoteAndWatchNode();
                    return;
                /**结点已存在**/
                case NODEEXISTS:
                    watchNode();
                    return;
                /**创建成功**/
                case OK:
                    watchNode();
                    break;
                default:
                    log.error("some thing went wrong:", KeeperException.create(KeeperException.Code.get(rc), path));
                    break;
            }
        }
    };
}
