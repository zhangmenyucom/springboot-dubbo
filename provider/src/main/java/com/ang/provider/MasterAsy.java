package com.ang.provider;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

/**
 * @author xiaolu.zhang
 * @desc:
 * @date: 2018/3/1 11:40
 */
@Data
@Slf4j
public class MasterAsy implements Watcher {
    private ZooKeeper zooKeeper;
    private String hostPort;
    private String serverId = Integer.toHexString(new Random().nextInt());
    private boolean isLeader = Boolean.FALSE;

    private AsyncCallback.StringCallback createParentCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    createParent(path,(byte[])ctx);
                    return;
                case OK:
                    log.info("parent created");
                    break;
                case NODEEXISTS:
                    log.warn("parent already registered:{}", path);
                    break;
                default: log.error("some thing went wrong:",KeeperException.create(KeeperException.Code.get(rc),path));
            }
        }
    };
    private AsyncCallback.DataCallback masterCheckCallBack = new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    checkMaster();
                    return;
                case NONODE:
                    runForMaster();
                    break;
            }
        }
    };
    private AsyncCallback.StringCallback masterCreateCallBack = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    checkMaster();
                    return;
                case OK:
                    isLeader = true;
                    break;
                default:
                    isLeader = false;
            }
            System.out.println("I am " + (isLeader ? "" : "not") + " the leader");
        }
    };

    MasterAsy(String hostPort) {
        this.hostPort = hostPort;
    }

    void checkMaster() {
        zooKeeper.getData("/master", false, masterCheckCallBack, null);
    }

    public void runForMaster() {

        //zooKeeper.create("/master", serverId.getBytes(), OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, masterCreateCallBack, null);
    }

    public void startZK() throws IOException, KeeperException, InterruptedException {
        zooKeeper = new ZooKeeper(hostPort, 15000, this);
        bootstrap();
        zooKeeper.getChildren("/META-INF/dubbo",true);
    }

    public void stopZK() throws InterruptedException {
        zooKeeper.close();
    }

    @Override
    public void process(WatchedEvent watchedEvent) {

        System.out.println(watchedEvent+"++++++++++++++++++++++");
        List<String> children = null;
        try {
            children = zooKeeper.getChildren("/META-INF/dubbo", true);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(children.toString());

    }

    public static void main(String... args) throws Exception {
        MasterAsy m = new MasterAsy("111.231.140.42:2181,111.231.140.42:2182,111.231.140.42:2183");
        m.startZK();
        m.runForMaster();

        if (m.isLeader) {
            System.out.println("I am the leader");
            Thread.sleep(60000);
        } else {
            System.out.println("some one else is the leader");
        }
        List<String> children = m.getZooKeeper().getChildren("/META-INF/dubbo", true);
        System.out.println(children.toString());
        Thread.sleep(600000);
        m.stopZK();
    }

    public void bootstrap() {
        createParent("/workers", new byte[0]);
        createParent("/assign", new byte[0]);
        createParent("/tasks", new byte[0]);
        createParent("/status", new byte[0]);
        createParent("/master", new byte[0]);
        createParent("/META-INF/dubbo", new byte[0]);
    }

    public void createParent(String path, byte[] data) {
        zooKeeper.create(path, data, OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, createParentCallback, data);
    }


}
