package com.ang.provider;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;

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

    /**
     * 从节点注册到主节点，等待主节点分配任务
     **/
    public void register() {
        zooKeeper.create("/workers/" + name, name.getBytes(), OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, registerCallBack, null);
    }

    private AsyncCallback.StringCallback registerCallBack = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String callBackname) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    register();
                    break;
                case OK:
                    /**注册成功,监控assign节点，等待分配任务**/
                    log.info("registered successfully:{}", name);
                    watchAssign();
                    break;
                case NODEEXISTS:
                    log.info("Already registered:{}", name);
                    watchAssign();
                    break;
                case NONODE:
                    register();
                    break;
                default:
                    log.error("some thing went wrong:", KeeperException.create(KeeperException.Code.get(rc), path));
                    break;
            }
        }
    };

    /**
     * 监察主节点分配任务
     **/
    private void watchAssign() {
        zooKeeper.getChildren("/assign", assgignWather, watchAssignCallBack, null);
    }

    private Watcher assgignWather = new Watcher() {
        @Override
        public void process(WatchedEvent watchedEvent) {
            if (watchedEvent.getType() == Event.EventType.NodeCreated) {
                getWork();
            }
            watchAssign();
        }
    };

    public void getWork() {
        zooKeeper.getChildren("/assign", false, getWorkCallBack, null);
    }

    private AsyncCallback.ChildrenCallback getWorkCallBack = new AsyncCallback.ChildrenCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, List<String> list) {
            if (!list.isEmpty()) {
                for (String work : list) {
                    if (work.contains(name)) {
                        processWork(path + "/" + work);
                    }
                }
            }

        }
    };

    public void processWork(String workPath) {
        log.info("{}正在处理任务，path:{}", name, workPath);
        zooKeeper.getData(workPath, false, getWorkDataCallBack, workPath);
    }

    private AsyncCallback.DataCallback getWorkDataCallBack = new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            try {
                zooKeeper.delete(path, stat.getVersion());
                zooKeeper.delete("/tasks/" + new String(data).split("-")[0], 0);
                log.info("{}处理完毕，path:{}", name, path);
            } catch (KeeperException.NoNodeException ignored) {

            } catch (InterruptedException | KeeperException e) {
                e.printStackTrace();
            }
        }
    };


    private AsyncCallback.ChildrenCallback watchAssignCallBack = new AsyncCallback.ChildrenCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, List<String> children) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    log.info("链接丢失重新监视");
                    watchAssign();
                    break;
                case NONODE:
                    watchAssign();
                    break;
                case OK:
                    log.info("{}正在监控assign,等待任务分配", name);
                    getWork();
                    break;
                default:
                    log.error("some thing went wrong:", KeeperException.create(KeeperException.Code.get(rc), path));
                    break;
            }
        }
    };


    @Override
    public void process(WatchedEvent watchedEvent) {
        log.info(watchedEvent.getType().toString());
    }

    public static void main(String[] args) throws InterruptedException {
        Worker worker1 = new Worker("111.231.140.42:2181,111.231.140.42:2182,111.231.140.42:2183", "worker1");
        Worker worker2 = new Worker("111.231.140.42:2181,111.231.140.42:2182,111.231.140.42:2183", "worker2");
        Worker worker3 = new Worker("111.231.140.42:2181,111.231.140.42:2182,111.231.140.42:2183", "worker3");
        Worker worker4 = new Worker("111.231.140.42:2181,111.231.140.42:2182,111.231.140.42:2183", "worker4");
        worker1.register();
        worker2.register();
        worker3.register();
        worker4.register();
        Thread.sleep(60000000);

    }
}
