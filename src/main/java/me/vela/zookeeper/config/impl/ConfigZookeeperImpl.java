/**
 * 
 */
package me.vela.zookeeper.config.impl;

import java.util.List;

import me.vela.zookeeper.config.Config;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;

/**
 * @author w.vela <vela@longbeach-inc.com>
 *
 * @date 2014年4月9日 上午10:51:18
 */
public final class ConfigZookeeperImpl implements Config {

    private static final String PREFIX = "/config/";

    private final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(getClass());

    private final CuratorFramework client;

    private final PathChildrenCache pathChildrenCache;

    public ConfigZookeeperImpl(CuratorFramework client) {
        try {
            this.client = client;
            pathChildrenCache = new PathChildrenCache(client, ZKPaths.makePath(PREFIX, null), true);
            pathChildrenCache.start();
            pathChildrenCache.rebuild();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <T> T get(ConfigKey<T> key) {
        ChildData childData = pathChildrenCache.getCurrentData(ZKPaths.makePath(PREFIX,
                key.configKey()));
        try {
            if (childData != null && childData.getData() != null) {
                return key.value(new String(childData.getData()));
            } else {
                return key.defaultValue();
            }
        } catch (Exception e) {
            logger.error("Ops, fail to get key:{}", key, e);
            return key.defaultValue();
        }
    }

    @Override
    public <T> void set(ConfigKey<T> key, String value) {
        String fullPath = ZKPaths.makePath(PREFIX, key.configKey());
        byte[] bytes = value.getBytes();
        try {
            try {
                client.setData().forPath(fullPath, bytes);
            } catch (KeeperException.NoNodeException e) {
                client.create().creatingParentsIfNeeded().forPath(fullPath, bytes);
            }
        } catch (Throwable e) {
            throw new RuntimeException("fail to set config:" + key.configKey() + "->" + value, e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> List<T> getList(ConfigKey<List<Object>> key) {
        return (List<T>) get(key);
    }

}
