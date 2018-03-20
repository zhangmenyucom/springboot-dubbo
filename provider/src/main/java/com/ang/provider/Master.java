package com.ang.provider;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

/**
 * @author xiaolu.zhang
 * @desc:
 * @date: 2018/3/16 15:04
 */
@Data
@Slf4j
public class Master implements Watcher {
    private ZooKeeper zooKeeper;

    private String hostPort;

    private String name;

    private static final String NODE_NAME = "master";

    private boolean isLeader = Boolean.FALSE;

    private List<String> workerCacheList;


    private Watcher masterCheckWather = new Watcher() {
        @Override
        public void process(WatchedEvent watchedEvent) {
            if (watchedEvent.getType() == Event.EventType.NodeDeleted) {
                if (("/" + NODE_NAME).equals(watchedEvent.getPath())) {
                    runForMaster();
                }
            }
        }
    };

    public Master(String hostPort, String name) {
        this.hostPort = hostPort;
        this.name = name;
        try {
            zooKeeper = new ZooKeeper(hostPort, 60000, this);
        } catch (IOException e) {
            log.info("zookeeper创建失败");
            e.printStackTrace();
        }
    }

    public void checkMaster() {
        zooKeeper.getData("/" + NODE_NAME, masterCheckWather, checkMasterCallBack, name);
    }

    public void runForMaster() {
        zooKeeper.create("/" + NODE_NAME, name.getBytes(), OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, masterCreateCallBack, name);
    }

    Watcher workersChangeWather = new Watcher() {
        @Override
        public void process(WatchedEvent watchedEvent) {
            if (watchedEvent.getType() == Event.EventType.NodeChildrenChanged) {
                if ("/workers".equals(watchedEvent.getPath())) {
                    getWorkers();
                }
            }
        }
    };

    private void getWorkers() {
        zooKeeper.getChildren("/workers", workersChangeWather, workersGetChildrenCallBack, null);
    }

    private AsyncCallback.ChildrenCallback workersGetChildrenCallBack = new AsyncCallback.ChildrenCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, List<String> list) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    getWorkers();
                    break;
                case OK:
                    workerCacheList = list;
                    reassignAndSet(workerCacheList);
                    break;
                default:
                    log.error("some thing went wrong:", KeeperException.create(KeeperException.Code.get(rc), path));
                    break;
            }
        }
    };

    private void reassignAndSet(List<String> children) {
        if (workerCacheList == null || workerCacheList.isEmpty()) {
            try {
                log.info("暂时没有worker");
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            getWorkers();
        } else {
            try {
                List<String> taskList = zooKeeper.getChildren("/tasks", false);
                for (String task : taskList) {
                    checkAndAssign(task);
                }
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
            }

        }
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
                    if (stat == null) {
                        runForMaster();
                        break;
                    }
                    if (name.equals(new String(data))) {
                        isLeader = Boolean.TRUE;
                        bootstrap();
                    } else {
                        isLeader = Boolean.FALSE;
                    }
                    log.info("{}竞选主节点{}", name, isLeader ? "成功" : "失败");
                    log.info("{} is {} the leader", name, isLeader ? "" : "not");
                    break;
                default:
                    checkMaster();
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
                    checkMaster();
                    break;
                case NODEEXISTS:
                    checkMaster();
                    break;
                case OK:
                    checkMaster();
                    break;
                default:
                    checkMaster();
                    break;
            }
        }
    };

    /**
     * 主结节初始化根节点
     **/
    public void bootstrap() {
        createParent("/workers", "workers".getBytes());
        createParent("/assign", "assign".getBytes());
        createParent("/tasks", "tasks".getBytes());
        getWorkers();
    }

    /**
     * 创建根节点
     **/
    public void createParent(String path, byte[] data) {
        zooKeeper.create(path, data, OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, createParentCallback, new String(data));
    }

    /**
     * 创建根节点回调
     **/
    private AsyncCallback.StringCallback createParentCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    createParent(path, (byte[]) ctx);
                    break;
                case OK:
                    log.info("{} node created", ctx);
                    if ("tasks".equals(ctx)) {
                        watchTasks();
                    }
                    break;
                case NODEEXISTS:
                    log.info("{} node already exists:{}", ctx, path);
                    if ("tasks".equals(ctx)) {
                        watchTasks();
                    }
                    break;
                default:
                    log.error("some thing went wrong:", KeeperException.create(KeeperException.Code.get(rc), path));
                    createParent(path, (byte[]) ctx);
            }
        }
    };

    public void watchTasks() {
        zooKeeper.getChildren("/tasks", new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                if (watchedEvent.getType() == Event.EventType.NodeChildrenChanged) {
                    zooKeeper.getChildren("/tasks", false, tasksChangeGetCallBack, null);
                }
            }
        }, watchTasksCallBack, null);
    }

    private AsyncCallback.ChildrenCallback tasksChangeGetCallBack = new AsyncCallback.ChildrenCallback() {

        @Override
        public void processResult(int rc, String path, Object ctx, List<String> list) {
            for (String work : list) {
                checkAndAssign(work);
            }
            watchTasks();
        }
    };

    public void checkAndAssign(String work) {
        Map<String, Object> map = new HashMap<>(0);
        map.put("work", work);
        while (workerCacheList.isEmpty()) {
            getWorkers();
            try {
                Thread.sleep(6000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        String worker = workerCacheList.get(new Random().nextInt(workerCacheList.size()));
        map.put("worker", worker);
        zooKeeper.getData("/assign/" + worker, false, checkAndAssignDataCallBack, map);

    }

    private AsyncCallback.DataCallback checkAndAssignDataCallBack= new AsyncCallback.DataCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                Map<String, String> map = (Map<String, String>) ctx;
                switch (KeeperException.Code.get(rc)) {
                    case CONNECTIONLOSS:
                        checkAndAssign(map.get("work"));
                        break;
                    case OK:
                        break;
                    case NONODE:
                        assinWork(map);
                        break;
                    default:
                        log.error("some thing went wrong:", KeeperException.create(KeeperException.Code.get(rc), path));
                        checkAndAssign((String) ctx);
                        break;
                }
            }
        };

    public void assinWork(Map<String, String> map) {
        zooKeeper.create("/assign/" + map.get("worker"), (map.get("work") + "-todo").getBytes(), OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, assignWorkCallback, map);
    }


    /**
     * 分配任务回调
     **/
    private AsyncCallback.StringCallback assignWorkCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            Map<String, String> map = (Map<String, String>) ctx;
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    assinWork(map);
                    break;
                case OK:
                    log.info("任务{}已分配给了{}", map.get("work"), map.get("worker"));
                    break;
                case NODEEXISTS:
                    checkAndAssign(map.get("work"));
                    break;
                default:
                    log.error("some thing went wrong:", KeeperException.create(KeeperException.Code.get(rc), path));
                    createParent(path, ctx.toString().getBytes());
            }
        }
    };


    private AsyncCallback.ChildrenCallback watchTasksCallBack = new AsyncCallback.ChildrenCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, List<String> children) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    log.info("链接丢失重新监视");
                    watchTasks();
                    break;
                case OK:
                    log.info("{}已监控tasks,正在等待任务到来", name);
                    break;
                default:
                    log.error("some thing went wrong:", KeeperException.create(KeeperException.Code.get(rc), path));
                    watchTasks();
                    break;
            }
        }
    };


    @Override
    public void process(WatchedEvent watchedEvent) {
        log.info(watchedEvent.getType().toString());

    }

    public static void main(String[] args) throws InterruptedException {
        Master master1 = new Master("111.231.140.42:2181,111.231.140.42:2182,111.231.140.42:2183", "master1");
        Master master2 = new Master("111.231.140.42:2181,111.231.140.42:2182,111.231.140.42:2183", "master2");
        Master master3 = new Master("111.231.140.42:2181,111.231.140.42:2182,111.231.140.42:2183", "master3");
        Master master4 = new Master("111.231.140.42:2181,111.231.140.42:2182,111.231.140.42:2183", "master4");
        master1.runForMaster();
        master2.runForMaster();
        master3.runForMaster();
        master4.runForMaster();
        Thread.sleep(6000000);
    }
}
