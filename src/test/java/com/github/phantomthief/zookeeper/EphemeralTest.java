package com.github.phantomthief.zookeeper;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.zookeeper.CreateMode.EPHEMERAL;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import org.junit.jupiter.api.Test;

/**
 * @author w.vela
 * Created on 2017-10-31.
 */
class EphemeralTest extends BaseTest {

    @Test
    void test() throws Exception {
        String path = "/ephemeral";
        byte[] value = "test".getBytes();
        curatorFramework.create().withMode(EPHEMERAL).forPath(path, value);
        assertArrayEquals(curatorFramework.getData().forPath(path), value);
        System.err.println("test equals.");
        testingServer.restart();
        sleepUninterruptibly(3, SECONDS);
        System.err.println("second...");
        assertArrayEquals(curatorFramework.getData().forPath(path), value);
    }
}
