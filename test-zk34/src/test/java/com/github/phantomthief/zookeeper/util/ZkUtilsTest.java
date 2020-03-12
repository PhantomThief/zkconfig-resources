package com.github.phantomthief.zookeeper.util;

import static com.github.phantomthief.zookeeper.util.ZkUtils.getAllChildren;
import static com.github.phantomthief.zookeeper.util.ZkUtils.getAllChildrenWithData;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.concurrent.TimeUnit.DAYS;
import static org.apache.curator.framework.CuratorFrameworkFactory.newClient;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.github.phantomthief.zookeeper.BaseTest;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * @author w.vela
 */
class ZkUtilsTest {

    private static final int LOOP = 100;
    private static final String TEST_PATH = "/test1/test";
    private static final String TEST_PATH2 = "/test2/test";

    private static ObjectMapper mapper;
    private static TestingServer testingServer;
    private static CuratorFramework curatorFramework;

    private static String otherConnectionStr = System.getProperty("zk.other.connectionStr", null);

    @BeforeAll
    static void init() throws Exception {
        mapper = new ObjectMapper();
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder()
            .retryPolicy(new RetryForever(1000));
        if (BaseTest.isZk34CompatibilityMode()) {
            builder.zk34CompatibilityMode(true);
        }
        if (otherConnectionStr == null) {
            testingServer = new TestingServer(true);
            builder.connectString(testingServer.getConnectString());
        } else {
            builder.connectString(otherConnectionStr);
        }
        curatorFramework = builder.build();
        curatorFramework.start();
    }

    @AfterAll
    static void destroy() throws IOException {
        curatorFramework.close();
        if (testingServer != null) {
            testingServer.close();
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static <T> T fromJSON(String json, Class<? extends Collection> collectionType,
            Class<?> valueType) {
        if (json == null || json.length() == 0) {
            try {
                return (T) collectionType.newInstance();
            } catch (Exception e) {
                throwIfUnchecked(e);
                throw new RuntimeException(e);
            }
        }
        try {
            return (T) mapper.readValue(json, TypeFactory.defaultInstance()
                    .constructCollectionType(collectionType, valueType));
        } catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    private static String toJSON(Object obj) {
        try {
            return mapper.writeValueAsString(obj);
        } catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    @Test
    void testGetAndSet() {
        ZkUtils.setToZk(curatorFramework, "/1", "2".getBytes());
        assertEquals(ZkUtils.getStringFromZk(curatorFramework, "/1"), "2");
    }

    @Test
    void testModify() throws Exception {
        ZkUtils.changeZkValue(curatorFramework, TEST_PATH, old -> {
            old.add("1");
            return old;
        }, this::setDecode, this::setEncode);
        assertEquals(setDecode(curatorFramework.getData().forPath(TEST_PATH)),
                Sets.newHashSet("1"));

        ZkUtils.changeZkValue(curatorFramework, TEST_PATH, old -> {
            old.remove("1");
            return old;
        }, this::setDecode, this::setEncode);
        assertEquals(setDecode(curatorFramework.getData().forPath(TEST_PATH)), Sets.newHashSet());
    }

    @Test
    void testConcurrent() throws Exception {
        ZkUtils.setToZk(curatorFramework, TEST_PATH2, "0".getBytes());
        ExecutorService executor = Executors.newFixedThreadPool(100);
        for (int i = 0; i < LOOP; i++) {
            executor.execute(() -> {
                ZkUtils.changeZkValue(curatorFramework, TEST_PATH2, old -> old + 1, this::intDecode,
                        this::intEncode);
            });
        }
        MoreExecutors.shutdownAndAwaitTermination(executor, 1, DAYS);
        assertEquals(intDecode(curatorFramework.getData().forPath(TEST_PATH2)), LOOP);
    }

    @Test
    void testGetAllChildren() {
        ZkUtils.setToZk(curatorFramework, "/all/test1/a", "a".getBytes());
        ZkUtils.setToZk(curatorFramework, "/all/test1/b", "b".getBytes());
        ZkUtils.setToZk(curatorFramework, "/all/test1/b/c", "c".getBytes());
        ZkUtils.setToZk(curatorFramework, "/all/test1/b/c/c1", "c1".getBytes());
        ZkUtils.setToZk(curatorFramework, "/all/test1/b/c/c2", "c2".getBytes());
        getAllChildren(curatorFramework, "/all/test1").forEach(System.out::println);
        assertEquals(5, getAllChildren(curatorFramework, "/all/test1").count());
        System.out.println("no end /");
        getAllChildren(curatorFramework, "/all/test1/").forEach(System.out::println);
        assertEquals(5, getAllChildren(curatorFramework, "/all/test1").count());
        System.out.println("no path");
        getAllChildren(curatorFramework, "/all/xyz/").forEach(System.out::println);
        assertEquals(0, getAllChildren(curatorFramework, "/all/xyz/").count());
    }

    @Test
    void testGetAllChildrenWithData() {
        ZkUtils.setToZk(curatorFramework, "/all/test3/a", "a".getBytes());
        ZkUtils.setToZk(curatorFramework, "/all/test3/b", "b".getBytes());
        ZkUtils.setToZk(curatorFramework, "/all/test3/b/c", "c".getBytes());
        ZkUtils.setToZk(curatorFramework, "/all/test3/b/c/c1", "c1".getBytes());
        ZkUtils.setToZk(curatorFramework, "/all/test3/b/c/c2", "c2".getBytes());
        getAllChildrenWithData(curatorFramework, "/all/test3").forEach(System.out::println);
        assertEquals(5, getAllChildrenWithData(curatorFramework, "/all/test3").count());
        System.out.println("no end /");
        getAllChildrenWithData(curatorFramework, "/all/test3/").forEach(System.out::println);
        assertEquals(5, getAllChildrenWithData(curatorFramework, "/all/test3").count());
        System.out.println("no path");
        getAllChildrenWithData(curatorFramework, "/all/xyz/").forEach(System.out::println);
        assertEquals(0, getAllChildrenWithData(curatorFramework, "/all/xyz/").count());
    }

    @Test
    void testGetAllChildrenShortOut() {
        ZkUtils.setToZk(curatorFramework, "/all/test2/a", "a".getBytes());
        ZkUtils.setToZk(curatorFramework, "/all/test2/b", "b".getBytes());
        ZkUtils.setToZk(curatorFramework, "/all/test2/b/c", "c".getBytes());
        ZkUtils.setToZk(curatorFramework, "/all/test2/b/c/c1", "c1".getBytes());
        ZkUtils.setToZk(curatorFramework, "/all/test2/b/c/c2", "c2".getBytes());
        assertEquals(2, getAllChildren(curatorFramework, "/all/test2").limit(2).count());
    }

    private int intDecode(byte[] raw) {
        if (raw == null) {
            return 0;
        } else {
            return Integer.parseInt(new String(raw));
        }
    }

    private byte[] intEncode(int i) {
        return String.valueOf(i).getBytes();
    }

    private Set<String> setDecode(byte[] raw) {
        if (raw == null) {
            return new HashSet<>();
        } else {
            return fromJSON(new String(raw), Set.class, String.class);
        }
    }

    private byte[] setEncode(Set<String> set) {
        if (set == null) {
            return null;
        } else {
            return toJSON(set).getBytes();
        }
    }
}
