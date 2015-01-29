/**
 * 
 */
package com.github.phantomthief.util;

import java.util.Arrays;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.data.Stat;

/**
 * @author w.vela
 */
public final class CuratorUtils {

    private static final int DEFAULT_RETRY_TIMES = 10;

    private static final int LOG_THRESHOLD = 10;

    private static final int LOG_FREQ = 10;

    private static org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CuratorUtils.class);

    private CuratorUtils() {
    }

    public static final void setData(Supplier<CuratorFramework> clientSupplier, String zkPath,
            Function<byte[], byte[]> updateOperation) {
        setData(clientSupplier, zkPath, updateOperation, DEFAULT_RETRY_TIMES);
    }

    public static final void setData(Supplier<CuratorFramework> clientSupplier, String zkPath,
            Function<byte[], byte[]> updateOperation, int maxRetryTimes) {
        int times = 0;

        while (true) {
            try {
                Stat stat = new Stat();
                byte[] forPath = clientSupplier.get().getData().storingStatIn(stat).forPath(zkPath);
                if (forPath == null) {
                    throw new RuntimeException("there is no path for:" + zkPath);
                }
                byte[] newData = updateOperation.apply(forPath);
                if (newData == null || Arrays.equals(forPath, newData)) {
                    break;
                }
                clientSupplier.get().setData().withVersion(stat.getVersion())
                        .forPath(zkPath, newData);
            } catch (Throwable e) {
                logger.error("fail to set data for {}", zkPath, e);
            } finally {
                if (times++ > LOG_THRESHOLD && times % LOG_FREQ == 0) {
                    logger.warn("fail to set data for {} for {} times.", zkPath, times);
                }
                if (times > maxRetryTimes) {
                    throw new RuntimeException("fail to set data for {}" + zkPath);
                }
            }
        }
    }
}
