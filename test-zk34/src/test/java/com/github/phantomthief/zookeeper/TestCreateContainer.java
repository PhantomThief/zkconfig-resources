package com.github.phantomthief.zookeeper;

import static org.apache.zookeeper.CreateMode.EPHEMERAL_SEQUENTIAL;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

/**
 * @author w.vela
 * Created on 2020-03-23.
 */
class TestCreateContainer extends BaseTest {

    @Test
    void test() throws Exception {
        String path = "/myTest/lock";
        String result = curatorFramework.create()
                .creatingParentContainersIfNeeded()
                .withProtection()
                .withMode(EPHEMERAL_SEQUENTIAL)
                .forPath(path);
        assertNotNull(curatorFramework.getData().forPath(result));
    }
}
