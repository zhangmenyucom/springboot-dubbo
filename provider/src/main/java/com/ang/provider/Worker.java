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
        zooKeeper.exists("/assign", this, watchAssignCallBack, null);
    }

    private AsyncCallback.StatCallback watchAssignCallBack = new AsyncCallback.StatCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, Stat stat) {
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
                    break;
                default:
                    log.error("some thing went wrong:", KeeperException.create(KeeperException.Code.get(rc), path));
                    break;
            }
        }
    };


    @Override
    public void process(WatchedEvent watchedEvent) {
        String path = watchedEvent.getPath();
        if ("/assign".equals(path)) {
            watchAssign();
        }
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
                if (path.contains("/assign")) {
                    log.info("任务到来了");
                    try {
                        List<String> tasksList = zooKeeper.getChildren("/assign", false);
                        for (String tasks : tasksList) {
                            if (tasks.contains(name)) {
                                log.info("{}正在处理{}", name, tasks);
                            }
                        }
                    } catch (KeeperException | InterruptedException e) {
                        e.printStackTrace();
                    }
                }
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
        worker1.register();
        worker2.register();
        worker3.register();
        worker4.register();
        Thread.sleep(60000000);

    }
}
