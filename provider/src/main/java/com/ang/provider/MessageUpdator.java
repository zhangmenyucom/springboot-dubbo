package com.ang.provider;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Random;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

/**
 * @author xiaolu.zhang
 * @desc:
 * @date: 2018/3/6 17:21
 */
@Data
@Slf4j
public class MessageUpdator implements Watcher {
    private ZooKeeper zk;
    private String hostport;
    private static String NOTE_NAME = "greyNode";

    MessageUpdator(String hostport) {
        this.hostport = hostport;
    }

    public void startZk() {
        try {
            /**创建zk实例**/
            zk = new ZooKeeper(hostport, 1500, this);
        } catch (IOException e) {
            log.info("zookeeperClient start failed");
            e.printStackTrace();
        }
    }

    /**
     * 创建标识结点
     **/
    private void checkNoteAndSendMessage() {
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
                    checkNoteAndSendMessage();
                    break;
                /**结点已存在**/
                case NODEEXISTS:
                    getUpdateZNodeData();
                    break;
                /**创建成功**/
                case OK:
                    getUpdateZNodeData();
                    break;
                default:
                    log.error("some thing went wrong:", KeeperException.create(KeeperException.Code.get(rc), path));
                    break;
            }
        }
    };

    /**
     * 获取节点数据
     **/
    private void getUpdateZNodeData() {
        zk.getData("/" + NOTE_NAME, this, dataCheckCallBack, null);
    }

    /**
     * 更新节点数据
     **/
    private void sendUpdateMassage(Stat stat) {
        zk.setData("/" + NOTE_NAME, Integer.toString(new Random().nextInt(100)).getBytes(), stat.getVersion(), updateCallBack, null);
    }

    /**
     * 获取数据后的回调函数
     **/
    private AsyncCallback.DataCallback dataCheckCallBack = new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                /**链接丢失重新获取**/
                case CONNECTIONLOSS:
                    getUpdateZNodeData();
                    break;
                /**节点被删除重新创建节点**/
                case NONODE:
                    checkNoteAndSendMessage();
                    break;
                /**获取节点成功后,更新数据**/
                case OK:
                    sendUpdateMassage(stat);
                    break;
                default:
                    log.error("some thing went wrong:", KeeperException.create(KeeperException.Code.get(rc), path));
                    break;
            }
        }
    };

    /**
     * 更新数据后的回调函数
     **/
    private AsyncCallback.StatCallback updateCallBack = new AsyncCallback.StatCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                /**链接丢失重新获取并发送数据**/
                case CONNECTIONLOSS:
                    getUpdateZNodeData();
                    break;
                /**节点被删除重新创建节点**/
                case NONODE:
                    checkNoteAndSendMessage();
                    break;
                /**获取更新成功后,输入日志**/
                case OK:
                    log.info("send update cache message success");
                    break;
                default:
                    log.error("some thing went wrong:", KeeperException.create(KeeperException.Code.get(rc), path));
                    break;
            }
        }
    };

    public static void main(String[] args) throws InterruptedException {
        MessageUpdator messageUpdator = new MessageUpdator("111.231.140.42:2181,111.231.140.42:2182,111.231.140.42:2183");
        messageUpdator.startZk();
        for (int i = 0; i < 10; i++) {
            messageUpdator.checkNoteAndSendMessage();
            Thread.sleep(2000);
        }
        Thread.sleep(100000);

    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println(watchedEvent);
    }
}
