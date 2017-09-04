package com.github.phantomthief.zookeeper;

import static com.github.phantomthief.zookeeper.util.ZkUtils.setToZk;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.junit.Test;

import com.github.phantomthief.util.ThrowableFunction;

/**
 * @author w.vela <wangtianzhou@kuaishou.com>
 * Created on 2017-09-04.
 */
public class ZkBaseNodeCloseTest extends BaseTest {

    @Test
    public void test() throws InterruptedException {
        boolean[] shutdown = { false };
        ThrowableFunction<String, String, Exception> func = i -> {
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
        try {
            testNode.close();
        } catch (IOException e) {
            fail(e.toString());
        }
        setToZk(curatorFramework, "/test", "test3".getBytes());
        SECONDS.sleep(10);
        try {
            testNode.get();
            fail();
        } catch (IllegalStateException e) {
            // ignore
        }
    }
}
