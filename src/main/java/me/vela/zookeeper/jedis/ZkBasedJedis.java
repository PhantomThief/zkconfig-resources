/**
 * 
 */
package me.vela.zookeeper.jedis;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.BaseStream;
import java.util.stream.Collectors;

import me.vela.util.ObjectMapperUtils;
import me.vela.zookeeper.AbstractZkBasedResource;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;

import redis.clients.jedis.BinaryJedisCommands;
import redis.clients.jedis.BinaryShardedJedis;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.Response;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPipeline;
import redis.clients.jedis.ShardedJedisPool;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;

/**
 * @author w.vela
 */
public class ZkBasedJedis extends AbstractZkBasedResource<ShardedJedisPool> {

    public static final int PARTITION_SIZE = 100;

    private static final int POOL_TOTAL_MAX_COUNT = 500;

    private static final int POOL_MAX_COUNT = 10;

    private final String monitorPath;

    private final PathChildrenCache cache;

    /**
     * @param monitorPath
     * @param cache
     */
    public ZkBasedJedis(String monitorPath, PathChildrenCache cache) {
        this.monitorPath = monitorPath;
        this.cache = cache;
    }

    /* (non-Javadoc)
     * @see me.vela.zookeeper.AbstractZkBasedResource#initObject(java.lang.String)
     */
    @Override
    protected ShardedJedisPool initObject(String rawNode) {
        try {
            Map<String, String> configs = ObjectMapperUtils.fromJSON(rawNode, Map.class,
                    String.class, String.class);

            JedisPoolConfig poolConfig = new JedisPoolConfig();
            poolConfig.setTestWhileIdle(true);
            poolConfig.setMaxTotal(POOL_TOTAL_MAX_COUNT);
            poolConfig.setMaxIdle(POOL_MAX_COUNT);
            poolConfig.setBlockWhenExhausted(true);

            BeanUtils.populate(poolConfig, configs);

            List<JedisShardInfo> shards = new ArrayList<JedisShardInfo>(configs.size());
            Splitter valueSplitter = Splitter.on(':');
            Splitter keySplitter = Splitter.on('@').limit(2);
            for (Entry<String, String> entry : configs.entrySet()) {
                List<String> split = valueSplitter.splitToList(entry.getValue());
                List<String> keySplit = keySplitter.splitToList(entry.getKey());
                JedisShardInfo jsi = new JedisShardInfo(split.get(0),
                        Integer.parseInt(split.get(1)), 0, keySplit.get(0));
                if (keySplit.size() == 2) {
                    jsi.setPassword(keySplit.get(1));
                }
                shards.add(jsi);
            }
            logger.info("build jedis for {}, {}", monitorPath, shards);
            return new ShardedJedisPool(poolConfig, shards);
        } catch (Throwable e) {
            logger.error("Ops. fail to get jedis pool:{}", monitorPath, e);
            throw new RuntimeException(e);
        }
    }

    /* (non-Javadoc)
     * @see me.vela.zookeeper.AbstractZkBasedResource#monitorPath()
     */
    @Override
    protected String monitorPath() {
        return monitorPath;
    }

    /* (non-Javadoc)
     * @see me.vela.zookeeper.AbstractZkBasedResource#cache()
     */
    @Override
    protected PathChildrenCache cache() {
        return cache;
    }

    /* (non-Javadoc)
     * @see me.vela.zookeeper.AbstractZkBasedResource#doCleanup(java.lang.Object)
     */
    @Override
    protected boolean doCleanup(ShardedJedisPool oldResource) {
        if (oldResource.isClosed()) {
            return true;
        }
        oldResource.close();
        return oldResource.isClosed();
    }

    public ShardedJedisPool getPool() {
        return getResource();
    }

    public JedisCommands get() {
        ShardedJedisPool thisPool = getPool();
        return (JedisCommands) Proxy.newProxyInstance(ShardedJedis.class.getClassLoader(),
                ShardedJedis.class.getInterfaces(), new PoolableJedisCommands(thisPool));
    }

    public BinaryJedisCommands getBinary() {
        ShardedJedisPool thisPool = getPool();
        return (BinaryJedisCommands) Proxy.newProxyInstance(
                BinaryShardedJedis.class.getClassLoader(),
                BinaryShardedJedis.class.getInterfaces(), new PoolableJedisCommands(thisPool));
    }

    public byte[] getToBytes(String key) {
        ShardedJedisPool pool = getPool();
        try (ShardedJedis shardedJedis = pool.getResource()) {
            return shardedJedis.get(key.getBytes());
        }
    }

    private final class PoolableJedisCommands implements InvocationHandler {

        private ShardedJedisPool jedis;

        /**
         * @param jedis
         */
        PoolableJedisCommands(ShardedJedisPool jedis) {
            this.jedis = jedis;
        }

        /* (non-Javadoc)
         * @see java.lang.reflect.InvocationHandler#invoke(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
         */
        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            try (ShardedJedis resource = jedis.getResource()) {
                Object invoke = method.invoke(resource, args);
                return invoke;
            }
        }
    }

    public <K> int zremByScore(String key, Collection<Double> scores) {
        int result = 0;
        ShardedJedisPool pool = getPool();
        try (ShardedJedis shardedJedis = pool.getResource()) {
            for (List<Double> list : Iterables.partition(scores, PARTITION_SIZE)) {
                ShardedJedisPipeline pipeline = shardedJedis.pipelined();
                List<Response<Long>> responseList = new ArrayList<>();
                for (Double score : list) {
                    responseList.add(pipeline.zremrangeByScore(key, score, score));
                }
                pipeline.sync();
                for (Response<Long> r : responseList) {
                    result += r.get();
                }
            }
        }
        return result;
    }

    public <K, V> Map<K, V> mget(Collection<K> keys, Function<K, String> keyGenerator,
            Function<String, V> codec) {
        Map<K, V> result = new HashMap<>(keys == null ? 0 : keys.size());
        if (keys != null) {
            Map<String, K> keyMap = new HashMap<>(keys.size());
            for (K key : keys) {
                keyMap.put(keyGenerator.apply(key), key);
            }
            for (List<Entry<String, K>> list : Iterables.partition(keyMap.entrySet(),
                    PARTITION_SIZE)) {
                ShardedJedisPool pool = getPool();
                try (ShardedJedis shardedJedis = pool.getResource()) {
                    ShardedJedisPipeline pipeline = shardedJedis.pipelined();
                    Map<K, Response<String>> responseMap = new HashMap<>(list.size());
                    for (Entry<String, K> entry : list) {
                        responseMap.put(entry.getValue(), pipeline.get(entry.getKey()));
                    }
                    pipeline.sync();
                    for (Entry<K, Response<String>> entry : responseMap.entrySet()) {
                        if (entry.getValue().get() != null) {
                            result.put(entry.getKey(), codec.apply(entry.getValue().get()));
                        }
                    }
                }
            }
        }
        return result;
    }

    public <K, V> Map<K, V> mgetByBytes(Collection<K> keys, Function<K, byte[]> keyGenerator,
            Function<byte[], V> codec) {
        Map<K, V> result = new HashMap<>(keys == null ? 0 : keys.size());
        if (keys != null) {
            Map<byte[], K> keyMap = new HashMap<>(keys.size());
            for (K key : keys) {
                keyMap.put(keyGenerator.apply(key), key);
            }
            for (List<Entry<byte[], K>> list : Iterables.partition(keyMap.entrySet(),
                    PARTITION_SIZE)) {
                ShardedJedisPool pool = getPool();
                try (ShardedJedis shardedJedis = pool.getResource()) {
                    ShardedJedisPipeline pipeline = shardedJedis.pipelined();
                    Map<K, Response<byte[]>> responseMap = new HashMap<>(list.size());
                    for (Entry<byte[], K> entry : list) {
                        responseMap.put(entry.getValue(), pipeline.get(entry.getKey()));
                    }
                    pipeline.sync();
                    for (Entry<K, Response<byte[]>> entry : responseMap.entrySet()) {
                        if (entry.getValue().get() != null) {
                            result.put(entry.getKey(), codec.apply(entry.getValue().get()));
                        }
                    }
                }
            }
        }
        return result;
    }

    public <K, V> Map<K, V> hmget(Collection<K> keys, Function<K, String> keyGenerator,
            Function<Map<String, String>, V> codec) {
        Map<K, V> result = new HashMap<>(keys == null ? 0 : keys.size());
        if (keys != null) {
            Map<String, K> keyMap = new HashMap<>(keys.size());
            for (K key : keys) {
                keyMap.put(keyGenerator.apply(key), key);
            }
            for (List<Entry<String, K>> list : Iterables.partition(keyMap.entrySet(),
                    PARTITION_SIZE)) {
                ShardedJedisPool pool = getPool();
                try (ShardedJedis shardedJedis = pool.getResource()) {
                    ShardedJedisPipeline pipeline = shardedJedis.pipelined();
                    Map<K, Response<Map<String, String>>> responseMap = new HashMap<>(list.size());
                    for (Entry<String, K> entry : list) {
                        responseMap.put(entry.getValue(), pipeline.hgetAll(entry.getKey()));
                    }
                    pipeline.sync();
                    for (Entry<K, Response<Map<String, String>>> entry : responseMap.entrySet()) {
                        if (entry.getValue().get() != null) {
                            result.put(entry.getKey(), codec.apply(entry.getValue().get()));
                        }
                    }
                }
            }
        }
        return result;
    }

    public <K, V> void mset(Map<K, V> map, Function<K, String> keyGenerator,
            Function<V, String> codec) {
        mset(map, keyGenerator, codec, 0);
    }

    public <K, V> void mset(Map<K, V> map, Function<K, String> keyGenerator,
            Function<V, String> codec, int expireSeconds) {
        if (MapUtils.isNotEmpty(map)) {
            Map<String, String> dataMap = map
                    .entrySet()
                    .stream()
                    .collect(
                            Collectors.toMap(entry -> keyGenerator.apply(entry.getKey()),
                                    entry -> codec.apply(entry.getValue())));
            for (List<Entry<String, String>> list : Iterables.partition(dataMap.entrySet(),
                    PARTITION_SIZE)) {
                ShardedJedisPool pool = getPool();
                try (ShardedJedis shardedJedis = pool.getResource()) {
                    ShardedJedisPipeline pipeline = shardedJedis.pipelined();
                    for (Entry<String, String> entry : list) {
                        if ((entry.getKey() != null) && (entry.getValue() != null)) {
                            if (expireSeconds > 0) {
                                pipeline.setex(entry.getKey(), expireSeconds, entry.getValue());
                            } else {
                                pipeline.set(entry.getKey(), entry.getValue());
                            }
                        }
                    }
                    pipeline.sync();
                }
            }
        }
    }

    public <K, V, E extends BaseStream<K, E>> Map<K, V> pipeline(E keys,
            BiFunction<ShardedJedisPipeline, K, Response<V>> function) {
        Map<K, V> result = new HashMap<>();
        if (keys != null) {
            Iterator<List<K>> partition = Iterators.partition(keys.iterator(), PARTITION_SIZE);
            for (Iterator<List<K>> iterator = partition; iterator.hasNext();) {
                List<K> list = iterator.next();
                ShardedJedisPool pool = getPool();
                try (ShardedJedis shardedJedis = pool.getResource()) {
                    ShardedJedisPipeline pipeline = shardedJedis.pipelined();
                    Map<K, Response<V>> thisMap = new HashMap<>(list.size());
                    for (K key : list) {
                        Response<V> apply = function.apply(pipeline, key);
                        thisMap.put(key, apply);
                    }
                    pipeline.sync();
                    for (Entry<K, Response<V>> entry : thisMap.entrySet()) {
                        if (entry.getValue() != null) {
                            result.put(entry.getKey(), entry.getValue().get());
                        }
                    }
                }
            }
        }
        return result;
    }

    public <K, V> Map<K, V> pipeline(Iterable<K> keys,
            BiFunction<ShardedJedisPipeline, K, Response<V>> function) {
        int size;
        if (keys != null && keys instanceof Collection) {
            size = ((Collection<K>) keys).size();
        } else {
            size = 16;
        }
        Map<K, V> result = new HashMap<>(size);
        if (keys != null) {
            Iterable<List<K>> partition = Iterables.partition(keys, PARTITION_SIZE);
            for (List<K> list : partition) {
                ShardedJedisPool pool = getPool();
                try (ShardedJedis shardedJedis = pool.getResource()) {
                    ShardedJedisPipeline pipeline = shardedJedis.pipelined();
                    Map<K, Response<V>> thisMap = new HashMap<>(list.size());
                    for (K key : list) {
                        Response<V> apply = function.apply(pipeline, key);
                        thisMap.put(key, apply);
                    }
                    pipeline.sync();
                    for (Entry<K, Response<V>> entry : thisMap.entrySet()) {
                        if (entry.getValue() != null) {
                            result.put(entry.getKey(), entry.getValue().get());
                        }
                    }
                }
            }
        }
        return result;
    }

    /**
     * 从redis里设置的bitset如果要读到Java里，一定要用这个，因为redis的存储和java的大小端模式不一样
     * 
     * @param bytes
     * @return
     */
    public static BitSet fromRedisBitSet(final byte[] bytes) {
        BitSet bits = new BitSet();
        if (bytes != null && bytes.length > 0) {
            for (int i = 0; i < (bytes.length * 8); i++) {
                if ((bytes[i / 8] & (1 << (7 - (i % 8)))) != 0) {
                    bits.set(i);
                }
            }
        }
        return bits;
    }

}
