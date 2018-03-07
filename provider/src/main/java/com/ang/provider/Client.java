package com.ang.provider;

import org.apache.zookeeper.*;

import java.io.IOException;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

/**
 * @author xiaolu.zhang
 * @desc:
 * @date: 2018/3/5 18:20
 */
public class Client implements Watcher {
    private ZooKeeper zk;
    private String hostPort;

    Client(String hostPort) {
        this.hostPort = hostPort;
    }

    void startZK() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println(watchedEvent);
    }

    String queueCommand(String command) throws Exception {
        while (true) {
            try {
                return zk.create("/tasks/task-", command.getBytes(), OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            } catch (KeeperException.NodeExistsException e) {
                throw new Exception("name already appears to be running");
            } catch (KeeperException.ConnectionLossException e) {

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Client c = new Client("111.231.140.42:2181,111.231.140.42:2182,111.231.140.42:2183");
        c.startZK();
        String name = c.queueCommand("heihei");
        System.out.println("create" + name);
    }
}
