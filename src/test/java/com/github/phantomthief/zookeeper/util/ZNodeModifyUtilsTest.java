/**
 * 
 */
package com.github.phantomthief.zookeeper.util;

import static java.util.concurrent.TimeUnit.DAYS;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.databind.type.TypeFactory;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * @author w.vela
 */
public class ZNodeModifyUtilsTest {

    private static final int LOOP = 100;
    private static final String TEST_PATH = "/test1/test";
    private static final String TEST_PATH2 = "/test2/test";

    public static com.fasterxml.jackson.databind.ObjectMapper mapper;
    private static TestingServer testingServer;
    private static CuratorFramework curatorFramework;

    @BeforeClass
    public static void init() throws Exception {
        mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        testingServer = new TestingServer(true);
        curatorFramework = CuratorFrameworkFactory.newClient(testingServer.getConnectString(),
                new ExponentialBackoffRetry(10000, 20));
        curatorFramework.start();
    }

    @AfterClass
    public static void destroy() throws IOException {
        curatorFramework.close();
        testingServer.close();
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static <T> T fromJSON(String json, Class<? extends Collection> collectionType,
            Class<?> valueType) {
        if (json == null || json.length() == 0) {
            try {
                return (T) collectionType.newInstance();
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }
        try {
            return (T) mapper.readValue(json, TypeFactory.defaultInstance()
                    .constructCollectionType(collectionType, valueType));
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    private static String toJSON(Object obj) {
        try {
            return mapper.writeValueAsString(obj);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Test
    public void testModify() throws Exception {
        ZNodeModifyUtils.modify(curatorFramework, TEST_PATH, old -> {
            old.add("1");
            return old;
        }, this::setDecode, this::setEncode);
        assertEquals(setDecode(curatorFramework.getData().forPath(TEST_PATH)),
                Sets.newHashSet("1"));

        ZNodeModifyUtils.modify(curatorFramework, TEST_PATH, old -> {
            old.remove("1");
            return old;
        }, this::setDecode, this::setEncode);
        assertEquals(setDecode(curatorFramework.getData().forPath(TEST_PATH)), Sets.newHashSet());
    }

    @Test
    public void testConcurrent() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(100);
        for (int i = 0; i < LOOP; i++) {
            executor.execute(() -> {
                ZNodeModifyUtils.modify(curatorFramework, TEST_PATH2, old -> old + 1,
                        this::intDecode, this::intEncode);
            });
        }
        MoreExecutors.shutdownAndAwaitTermination(executor, 1, DAYS);
        assertEquals(intDecode(curatorFramework.getData().forPath(TEST_PATH2)), LOOP);
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
