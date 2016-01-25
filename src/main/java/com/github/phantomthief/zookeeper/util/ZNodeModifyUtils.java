/**
 * 
 */
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

/**
 * @author w.vela
 */
public final class ZNodeModifyUtils {

    private static final long DEFAULT_WAIT = SECONDS.toMillis(1);
    private static final int INFINITY_LOOP = -1;
    private static Logger logger = getLogger(ZNodeModifyUtils.class);

    private ZNodeModifyUtils() {
        throw new UnsupportedOperationException();
    }

    public static void modify(CuratorFramework client, String path, byte[] value) {
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

    public static <T> void
            modify(CuratorFramework client, String path, Function<T, T> changeFunction,
                    Function<byte[], T> decoder, Function<T, byte[]> encoder) {
        Function<byte[], byte[]> realFunction = old -> {
            T decodedOld = decoder.apply(old);
            return encoder.apply(changeFunction.apply(decodedOld));
        };
        modify(client, path, realFunction, INFINITY_LOOP, DEFAULT_WAIT);
    }

    public static boolean modify(CuratorFramework client, String path,
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
