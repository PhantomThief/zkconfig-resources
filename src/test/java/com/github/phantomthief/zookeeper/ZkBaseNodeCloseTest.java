package com.github.phantomthief.zookeeper;

import static com.github.phantomthief.zookeeper.util.ZkUtils.setToZk;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.phantomthief.util.ThrowableFunction;

/**
 * @author w.vela
 * Created on 2017-09-04.
 */
public class ZkBaseNodeCloseTest extends BaseTest {

    private static final Logger logger = LoggerFactory.getLogger(ZkBaseNodeCloseTest.class);

    @Test
    public void test() throws InterruptedException {
        boolean[] shutdown = { false };
        ThrowableFunction<String, String, Exception> func = i -> {
            logger.info("found change:{}", i);
            if (shutdown[0]) {
                fail("shouldn't occurred.");
            }
            return i;
        };
        ZkBasedNodeResource<String> testNode = ZkBasedNodeResource.<String> newGenericBuilder() //
                .withCacheFactory("/test", curatorFramework) //
                .withStringFactoryEx(func) //
                .build();
        assertEquals(testNode.get(), "test1");
        setToZk(curatorFramework, "/test", "test2".getBytes());
        SECONDS.sleep(5);
        assertEquals(testNode.get(), "test2");
        testNode.close();
        shutdown[0] = true;
        
        setToZk(curatorFramework, "/test", "test3".getBytes());
        SECONDS.sleep(10);
        assertThrows(IllegalStateException.class, testNode::get);
    }
}
