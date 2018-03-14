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
 * @desc:触发器发送消息
 * @date: 2018/3/6 17:21
 */
@Data
@Slf4j
public class MessageUpdator implements Watcher {

    private ZooKeeper zk;

    private String zkServerList;

    private String nodeName = "greyNode";

    MessageUpdator(String zkServerList, String noteName) {
        try {
            this.nodeName = noteName;
            this.zkServerList = zkServerList;
            zk = new ZooKeeper(zkServerList, 60000, this);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 创建标识结点
     **/
    private void checkNoteAndSendMessage() {
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
                    checkNoteAndSendMessage();
                    break;
                /**结点已存在**/
                case NODEEXISTS:
                    getAndUpdateZNodeData();
                    break;
                /**创建成功**/
                case OK:
                    getAndUpdateZNodeData();
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
    public void getAndUpdateZNodeData() {
        zk.getData("/" + nodeName, this, dataCheckCallBack, null);
    }

    /**
     * 更新节点数据
     **/
    private void sendUpdateMassage(Stat stat) {
        log.info("正在发送更新请求");
        zk.setData("/" + nodeName, Integer.toString(new Random().nextInt(100)).getBytes(), stat.getVersion(), updateCallBack, null);
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
                    getAndUpdateZNodeData();
                    break;
                /**节点被删除重新创建节点**/
                case NONODE:
                    checkNoteAndSendMessage();
                    break;
                /**获取节点成功后,更新数据**/
                case OK:
                    sendUpdateMassage(stat);
                    break;
                /**超时重试**/
                case SESSIONEXPIRED:
                    getAndUpdateZNodeData();
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
                    getAndUpdateZNodeData();
                    break;
                /**节点被删除重新创建节点**/
                case NONODE:
                    checkNoteAndSendMessage();
                    break;
                /**获取更新成功后,输入日志**/
                case OK:
                    log.info("发送更新请求成功");
                    break;
                case BADVERSION:
                    log.info("已被其他进程更新,正在重试");
                    getAndUpdateZNodeData();
                    break;
                default:
                    log.error("some thing went wrong:", KeeperException.create(KeeperException.Code.get(rc), path));
                    break;
            }
        }
    };

    /**
     * 监控变化后的处理
     **/
    @Override
    public void process(WatchedEvent watchedEvent) {
        String path = watchedEvent.getPath();
        switch (watchedEvent.getType().getIntValue()) {
            case -1:
                log.info("");
                break;
            case 1:
                log.info("{} NodeCreated", path);
                break;
            case 2:
                log.info("{} NodeDeleted", path);
                break;
            case 3:
                log.info("{} NodeDataChanged", path);
                break;
            case 4:
                log.info("{} NodeChildrenChanged", path);
                break;
            default:
                break;
        }
    }
}
