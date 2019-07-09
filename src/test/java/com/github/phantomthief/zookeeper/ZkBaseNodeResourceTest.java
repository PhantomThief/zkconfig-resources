package com.github.phantomthief.zookeeper;

import static com.github.phantomthief.zookeeper.util.ZkUtils.setToZk;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.github.phantomthief.util.ThrowableFunction;

/**
 * ZkBaseNodeResourceTest
 * <p>
 * Write the code. Change the world.
 *
 * @author trang
 * @date 2019-02-11
 */
class ZkBaseNodeResourceTest extends BaseTest {

    private static final int EMPTY_VALUE = -1;

    @Test
    void testFailSafe() {
        String path = "/testFailSafe";
        ZkBasedNodeResource<Integer> node = ZkBasedNodeResource.<Integer>newGenericBuilder()
                .withCacheFactory(path, curatorFramework)
                .withStringFactoryEx((ThrowableFunction<String, Integer, Exception>) Integer::parseInt)
                .withEmptyObject(EMPTY_VALUE)
                .build();
        assertEquals(EMPTY_VALUE, node.get().intValue());

        setToZk(curatorFramework, path, "1".getBytes());
        sleepUninterruptibly(200, MILLISECONDS);
        assertEquals(1, node.get().intValue());

        setToZk(curatorFramework, path, "mytest".getBytes());
        sleepUninterruptibly(200, MILLISECONDS);
        // resource 构造失败后保持了上一次正确的值
        assertEquals(1, node.get().intValue());
    }

    /**
     * 测试默认的 refreshFactory（ImmediateFuture）下，当 resource 构造失败后是否能成功执行 factoryFailedListener
     * 直接返回 immediateFuture 是不能的
     */
    @Test
    void testOriginFactoryFailedListener() {
        String path = "/testOriginFactoryFailedListener";
        AtomicInteger counter = new AtomicInteger();
        ThrowableFunction<String, Integer, Exception> factory = Integer::parseInt;
        ZkBasedNodeResource<Integer> node = ZkBasedNodeResource.<Integer>newGenericBuilder()
                .withCacheFactory(path, curatorFramework)
                .withStringFactoryEx(factory)
                .withEmptyObject(EMPTY_VALUE)
                .withAsyncRefreshStringFactory(s -> immediateFuture(factory.apply(s))) // 构造之前版本的场景
                .addFactoryFailedListener((currentData, ex) -> counter.incrementAndGet())
                .build();
        assertEquals(EMPTY_VALUE, node.get().intValue());

        setToZk(curatorFramework, path, "1".getBytes());
        sleepUninterruptibly(200, MILLISECONDS);
        assertEquals(0, counter.get());

        setToZk(curatorFramework, path, "mytest".getBytes());
        sleepUninterruptibly(200, MILLISECONDS);
        // resource 构造失败后没有执行 factoryFailedListener
        assertEquals(0, counter.get());
    }

    /**
     * 测试默认的 refreshFactory（ImmediateFuture）下，当 resource 构造失败后是否能成功执行 factoryFailedListener
     * 通过线程池执行是可以的
     */
    @Test
    void testOriginFactoryFailedListenerWithExecutor() {
        String path = "/testOriginFactoryFailedListenerWithExecutor";
        AtomicInteger counter = new AtomicInteger();
        ThrowableFunction<String, Integer, Exception> factory = Integer::parseInt;
        ZkBasedNodeResource<Integer> node = ZkBasedNodeResource.<Integer>newGenericBuilder()
                .withCacheFactory(path, curatorFramework)
                .withStringFactoryEx(factory)
                .withEmptyObject(EMPTY_VALUE)
                .withRefreshStringFactory(listeningDecorator(newSingleThreadExecutor()), factory) // 构造之前版本的场景
                .addFactoryFailedListener((currentData, ex) -> counter.incrementAndGet())
                .build();
        assertEquals(EMPTY_VALUE, node.get().intValue());

        setToZk(curatorFramework, path, "1".getBytes());
        sleepUninterruptibly(200, MILLISECONDS);
        assertEquals(0, counter.get());

        setToZk(curatorFramework, path, "mytest".getBytes());
        sleepUninterruptibly(200, MILLISECONDS);
        assertEquals(1, counter.get());
    }

    /**
     * 测试默认的 refreshFactory（ImmediateFuture）下，当 resource 构造失败后是否能成功执行 factoryFailedListener
     * 分情况返回 immediateFuture or immediateFailedFuture 是可以的
     */
    @Test
    void testFactoryFailedListener() {
        String path = "/testFactoryFailedListener";
        AtomicInteger counter = new AtomicInteger();
        ZkBasedNodeResource<Integer> node = ZkBasedNodeResource.<Integer>newGenericBuilder()
                .withCacheFactory(path, curatorFramework)
                .withStringFactoryEx((ThrowableFunction<String, Integer, Exception>) Integer::parseInt)
                .withEmptyObject(EMPTY_VALUE)
                .addFactoryFailedListener((currentData, ex) -> counter.incrementAndGet())
                .build();
        assertEquals(EMPTY_VALUE, node.get().intValue());

        setToZk(curatorFramework, path, "1".getBytes());
        sleepUninterruptibly(200, MILLISECONDS);
        assertEquals(0, counter.get());

        setToZk(curatorFramework, path, "mytest".getBytes());
        sleepUninterruptibly(200, MILLISECONDS);
        assertEquals(1, counter.get());
    }

    /**
     * 测试默认的 refreshFactory（ImmediateFuture）下，当 resource 构造失败后是否能成功执行 factoryFailedListener
     * 通过线程池执行是可以的
     */
    @Test
    void testFactoryFailedListenerWithExecutor() {
        String path = "/testFactoryFailedListenerWithExecutor";
        AtomicInteger counter = new AtomicInteger();
        ThrowableFunction<String, Integer, Exception> factory = Integer::parseInt;
        ZkBasedNodeResource<Integer> node = ZkBasedNodeResource.<Integer>newGenericBuilder()
                .withCacheFactory(path, curatorFramework)
                .withStringFactoryEx(factory)
                .withEmptyObject(EMPTY_VALUE)
                .withRefreshStringFactory(listeningDecorator(newSingleThreadExecutor()), factory)
                .addFactoryFailedListener((currentData, ex) -> counter.incrementAndGet())
                .build();
        assertEquals(EMPTY_VALUE, node.get().intValue());

        setToZk(curatorFramework, path, "1".getBytes());
        sleepUninterruptibly(200, MILLISECONDS);
        assertEquals(0, counter.get());

        setToZk(curatorFramework, path, "mytest".getBytes());
        sleepUninterruptibly(200, MILLISECONDS);
        assertEquals(1, counter.get());
    }

}