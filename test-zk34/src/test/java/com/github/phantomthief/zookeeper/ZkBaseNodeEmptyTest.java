package com.github.phantomthief.zookeeper;

import static com.github.phantomthief.zookeeper.util.ZkUtils.removeFromZk;
import static com.github.phantomthief.zookeeper.util.ZkUtils.setToZk;
import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.ExecutorService;

import org.junit.jupiter.api.Test;

/**
 * @author w.vela
 * Created on 2017-12-25.
 */
class ZkBaseNodeEmptyTest extends BaseTest {

    @Test
    void test() {
        String path = "/emptyTest";
        String value = "TEST";
        ZkBasedNodeResource<String> node = ZkBasedNodeResource.<String> newGenericBuilder()
                .withCacheFactory(path, curatorFramework)
                .withStringFactoryEx(it -> it)
                .withEmptyObject(value)
                .build();
        ExecutorService executorService = newFixedThreadPool(1000);
        long s1 = currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {
            executorService.execute(() -> assertEquals(value, node.get()));
        }
        shutdownAndAwaitTermination(executorService, 1, DAYS);
        System.err.println("total cost:" + (currentTimeMillis() - s1));
        setToZk(curatorFramework, path, "mytest".getBytes());
        sleepUninterruptibly(1, SECONDS);
        assertEquals("mytest", node.get());

        removeFromZk(curatorFramework, path);
        sleepUninterruptibly(1, SECONDS);
        assertEquals(value, node.get());
    }
}
