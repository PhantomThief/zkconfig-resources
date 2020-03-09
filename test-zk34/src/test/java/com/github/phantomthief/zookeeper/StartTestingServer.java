package com.github.phantomthief.zookeeper;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.util.concurrent.TimeUnit.HOURS;

import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * @author w.vela
 * Created on 2020-03-09.
 */
class StartTestingServer {

    @Disabled
    @Test
    void test() throws Exception {
        TestingServer server = new TestingServer();
        server.start();
        System.out.println("zk3.4:" + server.getConnectString());
        sleepUninterruptibly(1, HOURS);
    }
}
