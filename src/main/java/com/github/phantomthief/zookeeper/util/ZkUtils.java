package com.github.phantomthief.zookeeper.util;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Throwables.propagate;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.Arrays;
import java.util.function.Function;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;

import com.github.phantomthief.util.ThrowableFunction;

/**
 * @author w.vela
 * Created on 03/12/2016.
 */
public class ZkUtils {

    private static final Logger logger = getLogger(ZNodeModifyUtils.class);

    private static final long DEFAULT_WAIT = SECONDS.toMillis(1);
    private static final int INFINITY_LOOP = -1;

    private ZkUtils() {
        throw new UnsupportedOperationException();
    }

    public static String getStringFromZk(CuratorFramework client, String path) {
        return getFromZk(client, path, String::new);
    }

    public static byte[] getBytesFromZk(CuratorFramework client, String path) {
        return getFromZk(client, path, b -> b);
    }

    public static <T, X extends Throwable> T getFromZk(CuratorFramework client, String path,
            ThrowableFunction<byte[], T, X> decoder) throws X {
        checkNotNull(client);
        checkNotNull(path);
        checkNotNull(decoder);
        try {
            byte[] bytes = client.getData().forPath(path);
            if (bytes == null) {
                return null;
            }
            return decoder.apply(bytes);
        } catch (KeeperException.NoNodeException e) {
            return null;
        } catch (Exception e) {
            throw propagate(e);
        }
    }

    public static void setToZk(CuratorFramework client, String path, byte[] value) {
        checkNotNull(client);
        checkNotNull(path);
        checkNotNull(value);
        try {
            client.setData().forPath(path, value);
        } catch (KeeperException.NoNodeException e) {
            try {
                client.create().creatingParentsIfNeeded().forPath(path, value);
            } catch (Exception e1) {
                throw propagate(e1);
            }
        } catch (Exception e) {
            throw propagate(e);
        }
    }

    public static <T> void changeZkValue(CuratorFramework client, String path,
            Function<T, T> changeFunction, Function<byte[], T> decoder,
            Function<T, byte[]> encoder) {
        Function<byte[], byte[]> realFunction = old -> {
            T decodedOld = decoder.apply(old);
            return encoder.apply(changeFunction.apply(decodedOld));
        };
        changeZkValue(client, path, realFunction, INFINITY_LOOP, DEFAULT_WAIT);
    }

    public static boolean changeZkValue(CuratorFramework client, String path,
            Function<byte[], byte[]> changeFunction, int retryTimes, long retryWait) {
        int times = 0;
        do {
            try {
                Stat stat = new Stat();
                byte[] oldData = client.getData().storingStatIn(stat).forPath(path);
                byte[] newData = changeFunction.apply(oldData);
                client.setData().withVersion(stat.getVersion()).forPath(path, newData);
                if (logger.isDebugEnabled()) {
                    logger.debug("success update znode:{} from {} to {}", path,
                            Arrays.toString(oldData), Arrays.toString(newData));
                }
                return true;
            } catch (KeeperException.BadVersionException e) {
                logger.debug("bad version for znode:{}, retry.{}", path, times);
            } catch (KeeperException.NoNodeException e) {
                byte[] newData = changeFunction.apply(null);
                try {
                    client.create().creatingParentsIfNeeded().forPath(path, newData);
                    if (logger.isDebugEnabled()) {
                        logger.debug("success create znode:{} -> {}", path,
                                Arrays.toString(newData));
                    }
                    return true;
                } catch (KeeperException.NodeExistsException ex) {
                    logger.debug("node exist for znode:{}, retry.{}", path, times);
                } catch (Exception ex) {
                    logger.error("Ops.{}/{}", path, times, ex);
                }
            } catch (Exception e) {
                logger.error("Ops.{}/{}", path, times, e);
                times++;
                sleepUninterruptibly(retryWait, MILLISECONDS);
            }
        } while (times < retryTimes || retryTimes == INFINITY_LOOP);
        logger.warn("fail to change znode:{}, retry times:{}", path, times);
        return false;
    }
}
