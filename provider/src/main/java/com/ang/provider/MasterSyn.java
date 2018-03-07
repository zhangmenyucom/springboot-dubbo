package com.ang.provider;

import lombok.Data;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Random;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

/**
 * @author xiaolu.zhang
 * @desc:
 * @date: 2018/3/1 11:40
 */
@Data
public class MasterSyn implements Watcher {
    private ZooKeeper zooKeeper;
    private String hostPort;
    private String serverId = Integer.toHexString(new Random().nextInt());
    private boolean isLeader = Boolean.FALSE;


    MasterSyn(String hostPort) {
        this.hostPort = hostPort;
    }

    public boolean checkMaster() {

        while (true) {
            try {
                Stat stat = new Stat();
                byte[] data = zooKeeper.getData("/master", false, stat);
                isLeader = new String(data).equals(serverId);
                return true;
            } catch (KeeperException.NoNodeException e) {
                return false;
            } catch (KeeperException.ConnectionLossException ignored) {

            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    public void runForMaster() {
        while (true) {
            try {
                zooKeeper.create("/master", serverId.getBytes(), OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                isLeader = true;
                break;
            } catch (KeeperException.NodeExistsException e) {
                isLeader = false;
                break;
            } catch (KeeperException.ConnectionLossException ignored) {

            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
            }
            if (checkMaster()) {
                break;
            }
        }
    }

    public void startZK() throws IOException {
        zooKeeper = new ZooKeeper(hostPort, 15000, this);
    }

    public void stopZK() throws InterruptedException {
        zooKeeper.close();
    }

    @Override
    public void process(WatchedEvent watchedEvent) {

        System.out.println(watchedEvent);

    }

    public static void main(String... args) throws Exception {
        MasterSyn m = new MasterSyn("111.231.140.42:2181,111.231.140.42:2182,111.231.140.42:2183");
        m.startZK();
        m.runForMaster();
        if (m.isLeader) {
            System.out.println("I am the leader");
            Thread.sleep(60000);
        } else {
            System.out.println("some one else is the leader");
        }
        m.stopZK();
    }


}
