package com.ang.provider;

import lombok.Data;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Date;

/**
 * @author xiaolu.zhang
 * @desc:
 * @date: 2018/3/6 11:22
 */
@Data
public class AdminClient implements Watcher {
    private ZooKeeper zooKeeper;
    private String hostPort;

    void start() throws IOException {
        zooKeeper=new ZooKeeper(hostPort,15000,this);
    }
    AdminClient(String hostPort){
        this.hostPort=hostPort;
    }
    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println(watchedEvent);
    }

    public void listState() throws KeeperException, InterruptedException {
        Stat stat=new Stat();
        byte[] masterData = zooKeeper.getData("/master", false, stat);
        Date startDate=new Date(stat.getCtime());
        System.out.println("Master:"+new String(masterData)+"since"+startDate);
        System.out.println("workers");
        for(String w: zooKeeper.getChildren("/workers",false)){
            byte[] data = zooKeeper.getData("/workers" + w, false, null);
            String state=new String(data);
            System.out.println("\t"+w+":"+state);
        }
        System.out.println("Tasks:");
        for(String t: zooKeeper.getChildren("/assign",false)){
            System.out.println("\t"+t);
        }
    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        AdminClient c=new AdminClient("111.231.140.42:2181,111.231.140.42:2182,111.231.140.42:2183");
        c.start();
        c.listState();
    }
}
