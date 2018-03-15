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
 * @date: 2018/3/15 17:15
 */
@Data
@Slf4j
public class Worker implements Watcher {

    private ZooKeeper zooKeeper;

    private String hostPort;

    private String name;

    private static final String NOTE_NAME = "master";

    private boolean isLeader = Boolean.FALSE;

    public Worker(String hostPort, String name) {
        this.hostPort = hostPort;
        this.name = name;
        try {
            zooKeeper = new ZooKeeper(hostPort, 1500, this);
        } catch (IOException e) {
            log.info("zookeeper创建失败");
            e.printStackTrace();
        }
    }

    public void checkMaster() {
        zooKeeper.getData("/" + NOTE_NAME, false, checkMasterCallBack, name);
    }

    public void runForMaster() {

        zooKeeper.create("/" + NOTE_NAME, name.getBytes(), OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, masterCreateCallBack, name);
    }

    /**
     * 获取数据后的回调函数
     **/
    private AsyncCallback.DataCallback checkMasterCallBack = new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    log.info("链接丢失重新checkMaster");
                    checkMaster();
                    break;
                case NONODE:
                    log.info("节点不存在竟选主节点");
                    runForMaster();
                    break;
                case OK:
                    if (name.equals(ctx)) {
                        isLeader = Boolean.TRUE;
                    } else {
                        isLeader = Boolean.FALSE;
                    }
                    log.info("竞选主节点{}", isLeader ? "成功" : "失败");
                    log.info("{} is {} the leader", name, isLeader ? "" : "not");
                    break;
                default:
                    log.error("some thing went wrong:", KeeperException.create(KeeperException.Code.get(rc), path));
                    break;
            }
        }
    };

    private AsyncCallback.StringCallback masterCreateCallBack = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String objct) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    log.info("链接丢失重新竟选");
                    runForMaster();
                    break;
                case NODEEXISTS:
                    checkMaster();
                    break;
                case OK:
                    checkMaster();
                    break;
                default:
                    isLeader = false;
                    break;
            }
        }
    };

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

    public static void main(String[] args) throws InterruptedException {
        Worker worker1 = new Worker("111.231.140.42:2181,111.231.140.42:2182,111.231.140.42:2183", "worker1");
        Worker worker2 = new Worker("111.231.140.42:2181,111.231.140.42:2182,111.231.140.42:2183", "worker2");
        Worker worker3 = new Worker("111.231.140.42:2181,111.231.140.42:2182,111.231.140.42:2183", "worker3");
        Worker worker4 = new Worker("111.231.140.42:2181,111.231.140.42:2182,111.231.140.42:2183", "worker4");
        worker1.runForMaster();
        worker2.runForMaster();
        worker3.runForMaster();
        worker4.runForMaster();
        Thread.sleep(6000000);
    }
}
