package com.github.phantomthief.zookeeper;

import static com.github.phantomthief.zookeeper.util.ZkUtils.removeFromZk;
import static org.apache.curator.framework.CuratorFrameworkFactory.newClient;

import java.io.IOException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
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
    private static String otherConnectionStr = System.getProperty("zk.other.connectionStr", null);
    private static Boolean zk34CompatibilityMode = Boolean.getBoolean("zk.zk34CompatibilityMode");

    public static Boolean isZk34CompatibilityMode() {
        return zk34CompatibilityMode;
    }

    @BeforeAll
    public static void init() throws Exception {
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory
            .builder()
            .retryPolicy(new RetryForever(1000));
        if (isZk34CompatibilityMode()) {
            builder.zk34CompatibilityMode(true);
        }
        if (otherConnectionStr == null) {
            testingServer = new TestingServer(true);
            builder.connectString(testingServer.getConnectString());
        } else {
            builder.connectString(otherConnectionStr);
        }
        curatorFramework = builder.build();
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

    public static void restartTestingServer() throws Exception {
        if (null != testingServer) {
            testingServer.restart();
        } else {
            throw new UnsupportedOperationException("Not allowed");
        }
    }

    public static void startTestingServer() throws Exception {
        if (null != testingServer) {
            testingServer.restart();
        } else {
            throw new UnsupportedOperationException("Not allowed");
        }
    }

    public static void stopTestingServer() throws Exception {
        if (null != testingServer) {
            testingServer.stop();
        } else {
            throw new UnsupportedOperationException("Not allowed");
        }
    }

    @AfterAll
    public static void destroy() throws IOException {
        curatorFramework.close();
        if (testingServer != null) {
            testingServer.close();
        }
    }
}
