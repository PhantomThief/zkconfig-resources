/**
 * 
 */
package com.github.phantomthief.zookeeper;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toMap;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author w.vela
 */
public class ZkBasedTreeNodeResourceTest {

    private static TestingServer testingServer;
    private static CuratorFramework curatorFramework;

    @BeforeClass
    public static void init() throws Exception {
        testingServer = new TestingServer(true);
        curatorFramework = CuratorFrameworkFactory.newClient(testingServer.getConnectString(),
                new ExponentialBackoffRetry(10000, 20));
        curatorFramework.start();
        curatorFramework.create().forPath("/test", "test1".getBytes());
        curatorFramework.create().forPath("/test/test1", "test1".getBytes());
        curatorFramework.create().forPath("/test/test2", "test2".getBytes());
        curatorFramework.create().forPath("/test/test3", "test3".getBytes());
        curatorFramework.create().forPath("/test/test3/test31", "test31".getBytes());
        curatorFramework.create().forPath("/test/test3/test32", "test32".getBytes());
        curatorFramework.create().forPath("/test/test3/test33", "test33".getBytes());
    }

    @AfterClass
    public static void destroy() throws IOException {
        curatorFramework.close();
        testingServer.close();
    }

    @Test
    public void test() throws Exception {
        ZkBasedTreeNodeResource<Map<String, String>> tree = ZkBasedTreeNodeResource
                .<Map<String, String>> newBuilder() //
                .curator(curatorFramework) //
                .path("/test") //
                .factory(p -> p.entrySet().stream()
                        .collect(toMap(Entry::getKey, e -> new String(e.getValue().getData())))) //
                .build();
        System.out.println(tree.get());
        curatorFramework.setData().forPath("/test/test3/test33", "test34".getBytes());
        sleepUninterruptibly(1, SECONDS);
        System.out.println(tree.get());
    }

    @Test
    public void testClose() throws Exception {
        ZkBasedTreeNodeResource<Map<String, String>> tree = ZkBasedTreeNodeResource
                .<Map<String, String>> newBuilder() //
                .curator(curatorFramework) //
                .path("/test") //
                .factory(p -> p.entrySet().stream()
                        .collect(toMap(Entry::getKey, e -> new String(e.getValue().getData())))) //
                .build();
        System.out.println(tree.get());
        tree.close();
        try {
            tree.get();
            fail();
        } catch (IllegalStateException e) {
            assertTrue(true);
        }
    }
}
