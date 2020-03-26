package com.github.phantomthief.zookeeper;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.curator.framework.recipes.cache.NodeCache;
import org.junit.jupiter.api.Test;

/**
 * @author w.vela
 * Created on 2020-03-26.
 */
class TestNodeParentCache extends BaseTest {

    @Test
    void test() throws Exception {
        String path = "/myTestNotExists/test1/test2";
        NodeCache cache = new NodeCache(curatorFramework, path);
        cache.start(true);
        assertNull(cache.getCurrentData());
        sleepUninterruptibly(20, SECONDS);
    }
}
