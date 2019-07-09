package com.github.phantomthief.zookeeper;

import static com.github.phantomthief.zookeeper.util.ZkUtils.setToZk;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;
import static org.apache.curator.utils.ZKPaths.makePath;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.curator.framework.recipes.cache.ChildData;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author w.vela
 */
class ZkBasedTreeNodeResourceTest extends BaseTest {

    private static final Logger logger = LoggerFactory.getLogger(ZkBasedTreeNodeResourceTest.class);

    @Test
    void test() throws Exception {
        ZkBasedTreeNodeResource<Map<String, String>> tree = ZkBasedTreeNodeResource
                .<Map<String, String>> newBuilder()
                .curator(curatorFramework)
                .path("/test")
                .factory(p -> p.entrySet().stream()
                        .collect(toMap(Entry::getKey, e -> new String(e.getValue().getData()))))
                .build();
        System.out.println(tree.get());
        curatorFramework.setData().forPath("/test/test3/test33", "test34".getBytes());
        sleepUninterruptibly(1, SECONDS);
        System.out.println(tree.get());
    }

    @Test
    void testClose() {
        ZkBasedTreeNodeResource<Map<String, String>> tree = ZkBasedTreeNodeResource
                .<Map<String, String>> newBuilder()
                .curator(curatorFramework)
                .path("/test")
                .factory(p -> p.entrySet().stream()
                        .collect(toMap(Entry::getKey, e -> new String(e.getValue().getData()))))
                .build();
        System.out.println(tree.get());
        tree.close();
        assertThrows(IllegalStateException.class, tree::get);
    }

    @Test
    void testAsyncRefresh() {
        String rootPath = "/testAsync";
        ZkBasedTreeNodeResource<String> node = ZkBasedTreeNodeResource.<String> newBuilder()
                .curator(curatorFramework)
                .path(rootPath)
                .factoryEx(map -> {
                    logger.info("build:{}=>{}", currentThread(), map);
                    sleepUninterruptibly(2, SECONDS);
                    return map.values().stream()
                            .map(ChildData::getData)
                            .map(String::new)
                            .collect(joining(","));
                })
                .build();
        ZkBasedTreeNodeResource<String> node2 = ZkBasedTreeNodeResource.<String> newBuilder()
                .curator(curatorFramework)
                .path(rootPath + "2")
                .factoryEx(map -> {
                    logger.info("build2:{}=>{}", currentThread(), map);
                    return map.values().stream()
                            .map(ChildData::getData)
                            .map(String::new)
                            .collect(joining(","));
                })
                .build();
        String path = makePath(rootPath, "test");
        String path2 = makePath(rootPath + "2", "test");
        setToZk(curatorFramework, path, "test1".getBytes());
        setToZk(curatorFramework, path2, "test1".getBytes());
        assertEquals("test1", node.get());
        assertEquals("test1", node2.get());

        setToZk(curatorFramework, path, "test2".getBytes());
        setToZk(curatorFramework, path2, "test2".getBytes());
        sleepUninterruptibly(1, SECONDS);
        assertEquals("test2", node2.get());

        setToZk(curatorFramework, path, "test3".getBytes());
        setToZk(curatorFramework, path2, "test3".getBytes());
        assertEquals("test1", node.get());
        sleepUninterruptibly(2, SECONDS);
        assertEquals("test2", node.get());
        assertEquals("test3", node2.get());

        sleepUninterruptibly(2, SECONDS);
        assertEquals("test3", node.get());
    }
}
