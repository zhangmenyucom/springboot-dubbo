package com.ang.provider;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Objects;
import java.util.Random;

/**
 * @author xiaolu.zhang
 * @desc:
 * @date: 2018/3/5 16:50
 */
@Data
@Slf4j
public class Worker implements Watcher {
    private ZooKeeper zooKeeper;
    private String hostpPort;
    private String serverId = Integer.toHexString(new Random().nextInt());
    private String status;
    private String name="worker-2084f557";

    private AsyncCallback.StatCallback statusUpdateCallBack = new AsyncCallback.StatCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    updateStatus((String) ctx);
            }
        }
    };

    private synchronized void updateStatus(String status) {
        if (Objects.equals(status, this.status)) {
            zooKeeper.setData("/workers/" + name, status.getBytes(), -1, statusUpdateCallBack, status);
        }
    }
    public void setStatus(String status){
        this.status=status;
        updateStatus(status);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        log.info("{},{}", watchedEvent.toString(), hostpPort);
    }

    Worker(String hostpPort) {
        this.hostpPort = hostpPort;
    }

    void startZK() throws IOException {
        zooKeeper = new ZooKeeper(hostpPort, 15000, this);
    }

    void register() {
        zooKeeper.create("/workers/worker-" + serverId, "Idle".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, createWorkerCallBack, null);
    }

    AsyncCallback.StringCallback createWorkerCallBack = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    register();
                    break;
                case OK:
                    log.info("regiestered successfully:{}", serverId);
                    break;
                case NODEEXISTS:
                    log.warn("already registersd:{}", serverId);
                    break;
                default:
                    log.error("something went wrong:{}", KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    public static void main(String[] args) throws IOException, InterruptedException {
        Worker w = new Worker("111.231.140.42:2181,111.231.140.42:2182,111.231.140.42:2183");
        w.startZK();
        w.register();
        w.updateStatus("test");
        Thread.sleep(30000);
    }
}
