package com.github.phantomthief.zookeeper;

import static com.github.phantomthief.zookeeper.util.ZkUtils.removeFromZk;
import static org.apache.curator.framework.CuratorFrameworkFactory.builder;
import static org.apache.curator.framework.CuratorFrameworkFactory.newClient;

import java.io.IOException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.retry.RetryForever;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

/**
 * @author w.vela
 * Created on 2017-08-25.
 */
public class BaseTest {

    protected static TestingServer testingServer;
    protected static CuratorFramework curatorFramework;
    private static String otherConnectionStr;
    private static boolean enableZk34Mode;

    @BeforeAll
    public static void init() throws Exception {
        if (otherConnectionStr == null) {
            testingServer = new TestingServer(true);
            curatorFramework = newClient(testingServer.getConnectString(), new RetryForever(1000));
        } else {
            if (enableZk34Mode) {
                curatorFramework = builder()
                        .connectString(otherConnectionStr)
                        .zk34CompatibilityMode(true)
                        .dontUseContainerParents()
                        .retryPolicy(new RetryForever(1000))
                        .build();
            } else {
                curatorFramework = newClient(otherConnectionStr, new RetryForever(1000));
            }
        }
        curatorFramework.start();
        removeFromZk(curatorFramework, "/test", true);
        curatorFramework.create().forPath("/test", "test1".getBytes());
        curatorFramework.create().forPath("/test/test1", "test1".getBytes());
        curatorFramework.create().forPath("/test/test2", "test2".getBytes());
        curatorFramework.create().forPath("/test/test3", "test3".getBytes());
        curatorFramework.create().forPath("/test/test3/test31", "test31".getBytes());
        curatorFramework.create().forPath("/test/test3/test32", "test32".getBytes());
        curatorFramework.create().forPath("/test/test3/test33", "test33".getBytes());
    }

    @AfterAll
    public static void destroy() throws IOException {
        curatorFramework.close();
        if (testingServer != null) {
            testingServer.close();
        }
    }
}
