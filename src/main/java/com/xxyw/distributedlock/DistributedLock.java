package com.xxyw.distributedlock;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;


public class DistributedLock {
    private final ZooKeeper zk;
    CountDownLatch connectLatch = new CountDownLatch(1);
    CountDownLatch waitLatch = new CountDownLatch(1);
    String waitPath;
    private String curNode;

    public DistributedLock() throws IOException, InterruptedException, KeeperException {
        // 连接ZooKeeper
        int sessionTimeout = 2000;
        String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
        zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                // 如果连接上ZooKeeper，释放connectLatch
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    connectLatch.countDown();
                }
                // 如果上一个节点释放，占用锁
                if (watchedEvent.getType() == Event.EventType.NodeDeleted && watchedEvent.getPath().equals(waitPath)) {
                    waitLatch.countDown();
                }
            }
        });

        // 等待连接后进行接下来的操作
        connectLatch.await();

        // 判断是否有 /lock目录，没有要创建
        Stat stat = zk.exists("/locks", false);
        if (stat == null) {
            zk.create("/locks", "locks".getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }

    // 获取资源，尝试上锁
    public void getLock() throws InterruptedException, KeeperException {
        // 创建节点
        curNode = zk.create("/locks/seq-", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        List<String> children = zk.getChildren("/locks", false);
        // 如果没有其他资源在排队等待，直接使用
        if (children.size() == 1) {
            return;
        }
        // 找到前一个序号的资源，监听它
        Collections.sort(children);
        int index = children.indexOf(curNode.substring("/locks/".length()));
        if (index == -1) {
            System.out.println("数据异常");
        } else if (index == 0) {
            return;
        } else {
            waitPath = "/locks/" + children.get(index - 1);
            zk.getData(waitPath, true, null);

            waitLatch.await();
            return;
        }
    }

    // 释放资源
    public void unlock() throws InterruptedException, KeeperException {
        zk.delete(curNode, -1);
    }
}
