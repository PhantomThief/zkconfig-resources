/**
 * 
 */
package com.github.phantomthief.zookeeper;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toMap;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import java.util.Map.Entry;

import org.junit.jupiter.api.Test;

/**
 * @author w.vela
 */
class ZkBasedTreeNodeResourceTest extends BaseTest {

    @Test
    void test() throws Exception {
        ZkBasedTreeNodeResource<Map<String, String>> tree = ZkBasedTreeNodeResource
                .<Map<String, String>> newBuilder() //
                .curator(curatorFramework) //
                .path("/test") //
                .factory(p -> p.entrySet().stream()
                        .collect(toMap(Entry::getKey, e -> new String(e.getValue().getData())))) //
                .build();
        System.out.println(tree.get());
        curatorFramework.setData().forPath("/test/test3/test33", "test34".getBytes());
        sleepUninterruptibly(1, SECONDS);
        System.out.println(tree.get());
    }

    @Test
    void testClose() throws Exception {
        ZkBasedTreeNodeResource<Map<String, String>> tree = ZkBasedTreeNodeResource
                .<Map<String, String>> newBuilder() //
                .curator(curatorFramework) //
                .path("/test") //
                .factory(p -> p.entrySet().stream()
                        .collect(toMap(Entry::getKey, e -> new String(e.getValue().getData())))) //
                .build();
        System.out.println(tree.get());
        tree.close();
        assertThrows(IllegalStateException.class, tree::get);
    }
}
