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
 * @date: 2018/3/20 16:30
 */
@Data
@Slf4j
public class TasksMaker implements Watcher {
    private ZooKeeper zooKeeper;

    private String hostPort;

    private String name;

    public TasksMaker(String hostPort, String name) {
        this.hostPort = hostPort;
        this.name = name;
        try {
            zooKeeper = new ZooKeeper(hostPort, 1500, this);
        } catch (IOException e) {
            log.info("zookeeper创建失败");
            e.printStackTrace();
        }
    }

    public void createTask(String content) {

        zooKeeper.getData("/tasks/task"+content, this, getDataCallBack, content);
    }

    private AsyncCallback.DataCallback getDataCallBack = new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                /**丢失链接重试**/
                case CONNECTIONLOSS:
                    createTask((String) ctx);
                    break;
                case OK:
                    log.info("任务--{}--已经存在", ctx);
                    break;
                case NONODE:
                    insertTasks((String) ctx);
                    break;
                default:
                    log.error("some thing went wrong:", KeeperException.create(KeeperException.Code.get(rc), path));
                    break;
            }
        }
    };

    /**
     * 插入新的任务
     **/
    private void insertTasks(String content) {
        zooKeeper.create("/tasks/task"+content, content.getBytes(), OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, insertTaskCallback, content);

    }

    private AsyncCallback.StringCallback insertTaskCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    insertTasks((String) ctx);
                    break;
                case OK:
                    log.info("任务{}创建成功", name);
                    break;
                default:
                    log.error("some thing went wrong:", KeeperException.create(KeeperException.Code.get(rc), path));
                    log.info("任务{}创建失败", name);
            }
        }
    };


    /**
     * 接受task节点发生的变化
     **/
    @Override
    public void process(WatchedEvent watchedEvent) {
        log.info(watchedEvent.getType().toString());
    }
}
