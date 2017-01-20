/**
 * 
 */
package com.github.phantomthief.zookeeper.test;

import static com.github.phantomthief.zookeeper.util.ZkUtils.removeFromZk;
import static com.github.phantomthief.zookeeper.util.ZkUtils.setToZk;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

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

    @AfterClass
    public static void destroy() throws IOException {
        curatorFramework.close();
        testingServer.close();
    }

    @Test
    public void testChange() throws Exception {
        ZkBasedNodeResource<String> node = ZkBasedNodeResource.<String> newGenericBuilder() //
                .withCacheFactory("/test", curatorFramework) //
                .withFactory((Function<byte[], String>) String::new) //
                .onResourceChange((current, old) -> {
                    System.out.println("current:" + current + ",old:" + old);
                    /*assertEquals(current, "test2");
                    assertEquals(old, "test1");*/
                }) //
                .build();
        System.out.println("current:" + node.get());
        assertEquals(node.get(), "test1");

        curatorFramework.setData().forPath("/test", "test2".getBytes());
        sleepUninterruptibly(1, SECONDS);
        System.out.println("current:" + node.get());
        assertEquals(node.get(), "test2");
        sleepUninterruptibly(5, SECONDS);
    }

    @Test
    public void testEmpty() throws Exception {
        ZkBasedNodeResource<String> node = ZkBasedNodeResource.newBuilder() //
                .withCacheFactory("/test2", curatorFramework) //
                .withFactory((Function<byte[], String>) String::new) //
                .withEmptyObject("EMPTY") //
                .build();
        System.out.println("current:" + node.get());
        assertEquals(node.get(), "EMPTY");

        curatorFramework.create().creatingParentsIfNeeded().forPath("/test2", "haha".getBytes());
        sleepUninterruptibly(1, SECONDS);
        System.out.println("current:" + node.get());
        assertEquals(node.get(), "haha"); //

        sleepUninterruptibly(1, SECONDS);
        curatorFramework.delete().forPath("/test2");
        sleepUninterruptibly(1, SECONDS);
        System.out.println("current:" + node.get());
        assertEquals(node.get(), "EMPTY");
    }

    @Test
    public void testClosed() throws Exception {
        ZkBasedNodeResource<String> node = ZkBasedNodeResource.newBuilder() //
                .withCacheFactory("/test2", curatorFramework) //
                .withFactory((Function<byte[], String>) String::new) //
                .withEmptyObject("EMPTY") //
                .build();
        assertEquals(node.get(), "EMPTY");
        node.close();
        try {
            node.get();
            fail();
        } catch (IllegalStateException e) {
            assertTrue(true);
        }
    }

    @Test
    public void testDeleteOnChange() throws Exception {
        AtomicInteger changed = new AtomicInteger();
        ZkBasedNodeResource<String> node = ZkBasedNodeResource.newBuilder() //
                .withCacheFactory("/test3", curatorFramework) //
                .withFactory((Function<byte[], String>) String::new) //
                .onResourceChange((n, o) -> {
                    System.out.println("old:" + o + ",new:" + n);
                    changed.incrementAndGet();
                }) //
                .withEmptyObject("EMPTY") //
                .build();
        String value = node.get();
        System.out.println("first empty value:" + value);
        assertEquals(value, "EMPTY");

        System.out.println("set..");
        setToZk(curatorFramework, "/test3", "abc".getBytes());
        sleepUninterruptibly(2, SECONDS);
        value = node.get();
        System.out.println("after set abc:" + value);
        assertEquals(value, "abc");

        System.out.println("delete...");
        curatorFramework.delete().forPath("/test3");
        sleepUninterruptibly(2, SECONDS);
        value = node.get();
        System.out.println("after delete:" + value);
        assertEquals(value, "EMPTY");

        System.out.println("set..");
        setToZk(curatorFramework, "/test3", "abc".getBytes());
        sleepUninterruptibly(2, SECONDS);
        value = node.get();
        System.out.println("after set abc:" + value);
        assertEquals(value, "abc");

        System.out.println("delete...");
        curatorFramework.delete().forPath("/test3");
        sleepUninterruptibly(2, SECONDS);
        value = node.get();
        System.out.println("after delete:" + value);
        assertEquals(value, "EMPTY");

        System.out.println("set..");
        setToZk(curatorFramework, "/test3", "abc".getBytes());
        sleepUninterruptibly(2, SECONDS);
        value = node.get();
        System.out.println("after set abc:" + value);
        assertEquals(value, "abc");

        System.out.println("set..");
        setToZk(curatorFramework, "/test3", "abcd".getBytes());
        sleepUninterruptibly(2, SECONDS);
        value = node.get();
        System.out.println("after set abc:" + value);
        assertEquals(value, "abcd");

        System.out.println("set..");
        setToZk(curatorFramework, "/test3", "abcd".getBytes());
        sleepUninterruptibly(2, SECONDS);
        value = node.get();
        System.out.println("after set abc:" + value);
        assertEquals(value, "abcd");

        System.out.println("changed:" + changed.get());
        assertTrue(changed.get() == 7);
    }

    @Test
    public void testExists() {
        ZkBasedNodeResource<String> node = ZkBasedNodeResource.newBuilder() //
                .withCacheFactory("/test5", curatorFramework) //
                .withFactory((Function<byte[], String>) String::new) //
                .withEmptyObject("EMPTY") //
                .build();

        assertFalse(node.zkNode().nodeExists());

        setToZk(curatorFramework, "/test5", "abc".getBytes());
        sleepUninterruptibly(1, SECONDS);

        assertTrue(node.zkNode().nodeExists());

        removeFromZk(curatorFramework, "/test5");
        sleepUninterruptibly(1, SECONDS);

        assertFalse(node.zkNode().nodeExists());
    }
}
