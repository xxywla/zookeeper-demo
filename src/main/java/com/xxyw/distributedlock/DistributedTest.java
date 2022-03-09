package com.xxyw.distributedlock;

import org.apache.zookeeper.KeeperException;

import java.io.IOException;

public class DistributedTest {
    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        DistributedLock lock1 = new DistributedLock();
        DistributedLock lock2 = new DistributedLock();

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    lock1.getLock();
                    System.out.println("线程1 启动，获取锁");
                    Thread.sleep(5000);
                    lock1.unlock();
                    System.out.println("线程1 释放锁");
                } catch (InterruptedException | KeeperException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    lock2.getLock();
                    System.out.println("线程2 启动，获取锁");
                    Thread.sleep(5000);
                    lock2.unlock();
                    System.out.println("线程2 释放锁");
                } catch (InterruptedException | KeeperException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }
}
