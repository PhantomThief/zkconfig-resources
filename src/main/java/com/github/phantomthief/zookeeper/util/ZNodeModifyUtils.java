/**
 * 
 */
package com.github.phantomthief.zookeeper.util;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.function.Function;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;

/**
 * moved to {@link com.github.phantomthief.zookeeper.util.ZkUtils}
 *
 * @author w.vela
 */
@Deprecated
public final class ZNodeModifyUtils {

    private static final long DEFAULT_WAIT = SECONDS.toMillis(1);
    private static final int INFINITY_LOOP = -1;
    private static Logger logger = getLogger(ZNodeModifyUtils.class);

    private ZNodeModifyUtils() {
        throw new UnsupportedOperationException();
    }

    /**
     * use {@link com.github.phantomthief.zookeeper.util.ZkUtils#setToZk} instead
     */
    @Deprecated
    public static void modify(CuratorFramework client, String path, byte[] value) {
        ZkUtils.setToZk(client, path, value);
    }

    /**
     * use {@link com.github.phantomthief.zookeeper.util.ZkUtils#changeZkValue instead
     */
    @Deprecated
    public static <T> void modify(CuratorFramework client, String path,
            Function<T, T> changeFunction, Function<byte[], T> decoder,
            Function<T, byte[]> encoder) {
        ZkUtils.changeZkValue(client, path, changeFunction, decoder, encoder);
    }

    /**
     * use {@link com.github.phantomthief.zookeeper.util.ZkUtils#changeZkValue instead
     */
    @Deprecated
    public static boolean modify(CuratorFramework client, String path,
            Function<byte[], byte[]> changeFunction, int retryTimes, long retryWait) {
        return ZkUtils.changeZkValue(client, path, changeFunction, retryTimes, retryWait);
    }
}
