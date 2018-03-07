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
    private String hostport;

    private static String NOTE_NAME = "greyNode";

    /**
     * 消息处理函数
     **/
    @Override
    public void process(WatchedEvent watchedEvent) {
        if (watchedEvent.getType() == Event.EventType.NodeDataChanged) {
            System.out.println("监控到有更新，开始更新缓存数据");
            /**zk机制一次出发，所以再次监听**/
            watchNode();
        }

    }

    MessageWatcher(String hostport) {
        this.hostport = hostport;
    }

    public void startZk() {
        try {
            /**创建zk实例**/
            zk = new ZooKeeper(hostport, 1500, this);
            /**创建结点并监控**/
            checkNoteAndWatchNode();
        } catch (IOException e) {
            log.info("zookeeperClient start failed");
            e.printStackTrace();
        }
    }

    /**
     * 获取数据并监控节点
     **/
    private void watchNode() {
        zk.getData("/" + NOTE_NAME, this, dataCheckCallBack, null);
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
                    watchNode();
                    return;
                /**节点被删除重新创建节点**/
                case NONODE:
                    checkNoteAndWatchNode();
                    break;
            }
        }
    };


    /**
     * 创建标识结点
     **/
    private void checkNoteAndWatchNode() {
        zk.create("/" + NOTE_NAME, "-1".getBytes(), OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, nodeCreateCallBack, null);
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
            }
        }
    };

    public static void main(String[] args) throws InterruptedException {
        MessageWatcher messageWatcher = new MessageWatcher("111.231.140.42:2181,111.231.140.42:2182,111.231.140.42:2183");
        messageWatcher.startZk();
        Thread.sleep(600000);

    }
}
