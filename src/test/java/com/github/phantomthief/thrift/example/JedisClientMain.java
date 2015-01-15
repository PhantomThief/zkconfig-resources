/**
 * 
 */
package com.github.phantomthief.thrift.example;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import com.github.phantomthief.zookeeper.jedis.ZkBasedJedis;

/**
 * @author w.vela
 */
public class JedisClientMain {

    public static void main(String[] args) throws IOException {
        try (CuratorFramework client = CuratorFrameworkFactory.newClient("127.0.0.1:2181",
                new ExponentialBackoffRetry(10000, 3));
                ZkBasedJedis jedis = new ZkBasedJedis("/jedis/clientConfig", client);) { // declare a jedis client using a config from zk's node.

            jedis.get().set("key1", "value1"); // exec jedis commands, don't worry about returning the connection back to the pool.
            System.out.println("get key:" + jedis.get().get("key1")); // exec jedis commands.

            // pipeline operation
            Random random = new Random();
            List<Integer> list = Arrays.asList(1, 2, 3, 4);

            jedis.pipeline(list,
                    (pipeline, key) -> pipeline.sadd("prefix:" + key, random.nextInt(100) + "")); // pipeline write

            Map<Integer, Long> multiCount = jedis.pipeline(list,
                    (pipeline, key) -> pipeline.scard("prefix:" + key)); // pipeline read
            System.out.println(multiCount);

            Map<Integer, Boolean> customCodecPipelineResult = jedis.pipeline(list,
                    (pipeline, key) -> pipeline.scard("prefix:" + key), value -> value > 10); // pipeline with customize decoder
            System.out.println(customCodecPipelineResult);
        }
    }
}
