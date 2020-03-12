package com.github.phantomthief.zookeeper;

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toMap;

import java.util.Map;

import org.junit.jupiter.api.Test;

/**
 * @author w.vela
 * Created on 2017-08-25.
 */
class ZkBaseTreeNodeExceptionTest extends BaseTest {

    @Test
    void testDisconnect() throws Exception {
        ZkBasedTreeNodeResource<Map<String, String>> tree = ZkBasedTreeNodeResource
                .<Map<String, String>> newBuilder()
                .curator(curatorFramework)
                .path("/test")
                .factoryEx(p -> p.entrySet().stream()
                        .collect(toMap(Map.Entry::getKey, e -> new String(e.getValue().getData()))))
                .build();
        System.out.println(tree.get());
        try {
            System.out.println("stop server.");
            stopTestingServer();
            System.out.println("after stop.");
            SECONDS.sleep(20);
        } catch (UnsupportedOperationException e) {
            System.out.println("Stop server is not allowed.");
        } finally {
            System.out.println(tree.get());
        }

        try {
            System.out.println("start server.");
            startTestingServer();
            System.out.println("after start.");
        } catch (UnsupportedOperationException e) {
            System.out.println("Start server is not allowed.");
        } finally {
            System.out.println(tree.get());
        }
    }
}
