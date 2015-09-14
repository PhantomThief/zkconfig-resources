/**
 * 
 */
package com.github.phantomthief.zookeeper.test;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.github.phantomthief.zookeeper.ZkBasedNodeResource;

/**
 * @author w.vela
 */
public class ZkNodeTest {

    private static TestingServer testingServer;
    private static CuratorFramework curatorFramework;

    @BeforeClass
    public static void init() throws Exception {
        testingServer = new TestingServer(true);
        curatorFramework = CuratorFrameworkFactory.newClient(testingServer.getConnectString(),
                new ExponentialBackoffRetry(10000, 20));
        curatorFramework.start();
        curatorFramework.create().forPath("/test", "test1".getBytes());
    }

    @Test
    public void testChange() throws Exception {
        ZkBasedNodeResource<String> node = ZkBasedNodeResource.<String> newBuilder() //
                .withCacheFactory("/test", curatorFramework) //
                .withFactory(bs -> new String(bs)) //
                .onResourceChange((current, old) -> {
                    System.out.println("current:" + current + ",old:" + old);
                    assert(current.equals("test2"));
                    assert(old.equals("test1"));
                }) //
                .build();
        System.out.println("current:" + node.get());
        assert(node.get().equals("test1"));
        curatorFramework.setData().forPath("/test", "test2".getBytes());
        sleepUninterruptibly(1, TimeUnit.SECONDS);
        System.out.println("current:" + node.get());
        assert(node.get().equals("test2"));
        sleepUninterruptibly(5, TimeUnit.SECONDS);
    }

    @Test
    public void testEmpty() throws Exception {
        ZkBasedNodeResource<String> node = ZkBasedNodeResource.<String> newBuilder() //
                .withCacheFactory("/test2", curatorFramework) //
                .withFactory(bs -> new String(bs)) //
                .withEmptyObject("EMPTY") //
                .build();
        System.out.println("current:" + node.get());
        assert(node.get().equals("EMPTY"));
        curatorFramework.create().creatingParentsIfNeeded().forPath("/test2", "haha".getBytes());
        sleepUninterruptibly(1, TimeUnit.SECONDS);
        System.out.println("current:" + node.get());
        assert(node.get().equals("haha")); //
        sleepUninterruptibly(1, TimeUnit.SECONDS);
        curatorFramework.delete().forPath("/test2");
        sleepUninterruptibly(1, TimeUnit.SECONDS);
        System.out.println("current:" + node.get());
        assert(node.get().equals("EMPTY"));
    }

    @AfterClass
    public static void destroy() throws IOException {
        curatorFramework.close();
        testingServer.close();
    }
}
