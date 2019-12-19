package com.github.phantomthief.zookeeper.test;

import static com.github.phantomthief.zookeeper.util.ZkUtils.removeFromZk;
import static com.github.phantomthief.zookeeper.util.ZkUtils.setToZk;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.phantomthief.zookeeper.ZkBasedNodeResource;

/**
 * @author w.vela
 */
class ZkNodeTest {

    private static final Logger logger = LoggerFactory.getLogger(ZkNodeTest.class);
    private static TestingServer testingServer;
    private static CuratorFramework curatorFramework;

    @BeforeAll
    static void init() throws Exception {
        testingServer = new TestingServer(true);
        curatorFramework = CuratorFrameworkFactory.newClient(testingServer.getConnectString(),
                new ExponentialBackoffRetry(10000, 20));
        curatorFramework.start();
        curatorFramework.create().forPath("/test", "test1".getBytes());
    }

    @AfterAll
    static void destroy() throws IOException {
        curatorFramework.close();
        testingServer.close();
    }

    @Test
    void testChange() throws Exception {
        ZkBasedNodeResource<String> node = ZkBasedNodeResource.<String> newGenericBuilder()
                .withCacheFactory("/test", curatorFramework)
                .withFactory((Function<byte[], String>) String::new)
                .onResourceChange((current, old) -> {
                    System.out.println("current:" + current + ",old:" + old);
                    /*assertEquals(current, "test2");
                    assertEquals(old, "test1");*/
                })
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
    void testEmpty() throws Exception {
        ZkBasedNodeResource<String> node = ZkBasedNodeResource.newBuilder()
                .withCacheFactory("/test2", curatorFramework)
                .withFactory((Function<byte[], String>) String::new)
                .withEmptyObject("EMPTY")
                .build();
        System.out.println("current:" + node.get());
        assertEquals(node.get(), "EMPTY");

        curatorFramework.create().creatingParentsIfNeeded().forPath("/test2", "haha".getBytes());
        sleepUninterruptibly(1, SECONDS);
        System.out.println("current:" + node.get());
        assertEquals(node.get(), "haha");

        sleepUninterruptibly(1, SECONDS);
        curatorFramework.delete().forPath("/test2");
        sleepUninterruptibly(1, SECONDS);
        System.out.println("current:" + node.get());
        assertEquals(node.get(), "EMPTY");
    }

    @Test
    void testClosed() throws Exception {
        ZkBasedNodeResource<String> node = ZkBasedNodeResource.newBuilder()
                .withCacheFactory("/test2", curatorFramework)
                .withFactory((Function<byte[], String>) String::new)
                .withEmptyObject("EMPTY")
                .build();
        assertEquals(node.get(), "EMPTY");
        node.close();
        assertThrows(IllegalStateException.class, node::get);
    }

    @Test
    void testDeleteOnChange() throws Exception {
        AtomicInteger changed = new AtomicInteger();
        ZkBasedNodeResource<String> node = ZkBasedNodeResource.newBuilder()
                .withCacheFactory("/test3", curatorFramework)
                .withFactory((Function<byte[], String>) String::new)
                .onResourceChange((n, o) -> {
                    System.out.println("old:" + o + ",new:" + n);
                    changed.incrementAndGet();
                })
                .withEmptyObject("EMPTY")
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
        assertEquals(7, changed.get());
    }

    @Test
    void testExists() {
        ZkBasedNodeResource<String> node = ZkBasedNodeResource.newBuilder()
                .withCacheFactory("/test5", curatorFramework)
                .withFactory((Function<byte[], String>) String::new)
                .withEmptyObject("EMPTY")
                .build();

        assertFalse(node.zkNode().nodeExists());

        setToZk(curatorFramework, "/test5", "abc".getBytes());
        sleepUninterruptibly(1, SECONDS);

        assertTrue(node.zkNode().nodeExists());

        removeFromZk(curatorFramework, "/test5");
        sleepUninterruptibly(1, SECONDS);

        assertFalse(node.zkNode().nodeExists());
    }

    @Test
    void testBuildThread() {
        String path = "/testThread";
        ZkBasedNodeResource<String> node = ZkBasedNodeResource.<String> newGenericBuilder()
                .withCacheFactory(path, curatorFramework)
                .withStringFactoryEx(n -> {
                    logger.info("build:{}=>{}", currentThread(), n);
                    return n;
                })
                .withEmptyObject("EMPTY")
                .build();
        setToZk(curatorFramework, path, "test1".getBytes());
        node.get();
        setToZk(curatorFramework, path, "test2".getBytes());
        sleepUninterruptibly(1, SECONDS);
        node.get();
        setToZk(curatorFramework, path, "test3".getBytes());
        node.get();
        sleepUninterruptibly(1, SECONDS);
    }

    @Test
    void testAsyncRefresh() {
        String path = "/testAsync";
        String path2 = "/testAsync2";
        ZkBasedNodeResource<String> node = ZkBasedNodeResource.<String> newGenericBuilder()
                .withCacheFactory(path, curatorFramework)
                .withStringFactoryEx(n -> {
                    logger.info("build:{}=>{}", currentThread(), n);
                    sleepUninterruptibly(2, SECONDS);
                    return n;
                })
                .asyncRefresh(listeningDecorator(newSingleThreadScheduledExecutor()))
                .withEmptyObject("EMPTY")
                .build();
        ZkBasedNodeResource<String> node2 = ZkBasedNodeResource.<String> newGenericBuilder()
                .withCacheFactory(path2, curatorFramework)
                .withStringFactoryEx(n -> {
                    logger.info("build2:{}=>{}", currentThread(), n);
                    return n;
                })
                .withEmptyObject("EMPTY")
                .build();
        setToZk(curatorFramework, path, "test1".getBytes());
        setToZk(curatorFramework, path2, "test1".getBytes());
        assertEquals("test1", node.get());
        assertEquals("test1", node2.get());

        setToZk(curatorFramework, path, "test2".getBytes());
        setToZk(curatorFramework, path2, "test2".getBytes());
        sleepUninterruptibly(1, SECONDS);
        assertEquals("test2", node2.get());

        setToZk(curatorFramework, path, "test3".getBytes());
        setToZk(curatorFramework, path2, "test3".getBytes());
        assertEquals("test1", node.get());
        sleepUninterruptibly(2, SECONDS);
        assertEquals("test2", node.get());
        assertEquals("test3", node2.get());
        sleepUninterruptibly(2, SECONDS);
        assertEquals("test3", node.get());
    }
}
