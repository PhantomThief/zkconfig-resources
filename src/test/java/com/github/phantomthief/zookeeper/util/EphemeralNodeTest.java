package com.github.phantomthief.zookeeper.util;

import static com.github.phantomthief.zookeeper.util.ZkUtils.KeepEphemeralListener;
import static com.github.phantomthief.zookeeper.util.ZkUtils.createEphemeralNode;
import static com.github.phantomthief.zookeeper.util.ZkUtils.getBytesFromZk;
import static com.github.phantomthief.zookeeper.util.ZkUtils.removeFromZk;
import static org.apache.curator.framework.state.ConnectionState.RECONNECTED;
import static org.apache.curator.test.KillSession.kill;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.phantomthief.zookeeper.BaseTest;

/**
 * @author w.vela
 * Created on 2017-11-04.
 */
class EphemeralNodeTest extends BaseTest {

    private static final Logger logger = LoggerFactory.getLogger(EphemeralNodeTest.class);

    @Test
    void test() throws Exception {
        String path = "/ephemeralNode";
        byte[] value = "test".getBytes();
        AutoCloseable node = createEphemeralNode(curatorFramework, path, value);
        assertArrayEquals(value, getBytesFromZk(curatorFramework, path));

        logger.info("kill session.");
        kill(curatorFramework.getZookeeperClient().getZooKeeper());
        logger.info("re-test");
        assertArrayEquals(value, getBytesFromZk(curatorFramework, path));

        removeFromZk(curatorFramework, path);
        assertNull(getBytesFromZk(curatorFramework, path));

        KeepEphemeralListener listener = KeepEphemeralListener.class.cast(node);
        listener.stateChanged(curatorFramework, RECONNECTED);
        assertArrayEquals(value, getBytesFromZk(curatorFramework, path));

        node.close();
        assertNull(getBytesFromZk(curatorFramework, path));
    }
}
