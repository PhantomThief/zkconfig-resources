package com.github.phantomthief.zookeeper;

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

    static TestingServer testingServer;
    protected static CuratorFramework curatorFramework;

    @BeforeAll
    public static void init() throws Exception {
        testingServer = new TestingServer(true);
        curatorFramework = CuratorFrameworkFactory.newClient(testingServer.getConnectString(),
                new RetryForever(1000));
        curatorFramework.start();
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
        testingServer.close();
    }
}
