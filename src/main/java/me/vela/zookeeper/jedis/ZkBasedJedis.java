/**
 * 
 */
package me.vela.zookeeper.jedis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import me.vela.util.ObjectMapperUtils;
import me.vela.zookeeper.AbstractZkBasedResource;

import org.apache.curator.framework.recipes.cache.PathChildrenCache;

import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedisPool;

import com.google.common.base.Splitter;

/**
 * @author w.vela
 */
public class ZkBasedJedis extends AbstractZkBasedResource<ShardedJedisPool> {

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

}
