package com.github.phantomthief.zookeeper;

import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author w.vela
 * Created on 2017-12-25.
 */
class ZkBaseNodeEmptyTest extends BaseTest {

    private static final Logger logger = LoggerFactory.getLogger(ZkBaseNodeEmptyTest.class);

    @Disabled
    @Test
    void test() {
        String path = "/emptyTest";
        String value = "TEST";
        ZkBasedNodeResource<String> node = ZkBasedNodeResource.<String> newGenericBuilder() //
                .withCacheFactory(path, curatorFramework) //
                .withStringFactoryEx(it -> it) //
                .withEmptyObject(value) //
                .build();
        ExecutorService executorService = newFixedThreadPool(300);
        for (int i = 0; i < 10000; i++) {
            executorService.execute(() -> {
                long s = currentTimeMillis();
                assertEquals(value, node.get());
                logger.info("cost:{}", currentTimeMillis() - s);
            });
        }
        shutdownAndAwaitTermination(executorService, 1, TimeUnit.DAYS);
    }
}
