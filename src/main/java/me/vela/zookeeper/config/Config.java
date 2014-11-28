/**
 * 
 */
package me.vela.zookeeper.config;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import me.vela.zookeeper.config.impl.ConfigZookeeperImpl;

import org.apache.curator.framework.CuratorFramework;

/**
 * @author w.vela <vela@longbeach-inc.com>
 *
 * @date 2014年4月9日 上午10:49:53
 */
public interface Config {

    public interface ConfigKey<T> {

        public String configKey();

        public T value(String rawValue) throws Exception;

        public T defaultValue();

        public default T get(CuratorFramework client) {
            return Config.CONFIGS.computeIfAbsent(client, ConfigZookeeperImpl::new).get(this);
        }

    }

    public static final ConcurrentMap<CuratorFramework, Config> CONFIGS = new ConcurrentHashMap<>();

    /**
     * 可以考虑直接使用#com.wvt.framework.config.Config.ConfigKey.get();
     * 
     * @param key
     * @return
     */
    public <T> T get(ConfigKey<T> key);

    public <R> List<R> getList(ConfigKey<List<Object>> key);

    public <T> void set(ConfigKey<T> key, String value);

}
