package com.github.phantomthief.zookeeper;

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toMap;

import java.util.Map;

import org.junit.Test;

/**
 * @author w.vela
 * Created on 2017-08-25.
 */
public class ZkBaseTreeNodeExceptionTest extends BaseTest {

    @Test
    public void testDisconnect() throws Exception {
        ZkBasedTreeNodeResource<Map<String, String>> tree = ZkBasedTreeNodeResource
                .<Map<String, String>> newBuilder() //
                .curator(curatorFramework) //
                .path("/test") //
                .factoryEx(p -> p.entrySet().stream()
                        .collect(toMap(Map.Entry::getKey, e -> new String(e.getValue().getData())))) //
                .build();
        System.out.println(tree.get());
        System.out.println("stop server.");
        testingServer.stop();
        System.out.println("after stop.");
        SECONDS.sleep(20);
        System.out.println(tree.get());
        System.out.println("start server.");
        testingServer.start();
        System.out.println("after start.");
        System.out.println(tree.get());
    }
}
