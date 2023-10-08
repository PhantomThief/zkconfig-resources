package com.github.phantomthief.zookeeper.util;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Stream.concat;
import static java.util.stream.Stream.empty;
import static org.apache.commons.lang3.StringUtils.removeEnd;
import static org.apache.commons.lang3.StringUtils.removeStart;
import static org.apache.curator.framework.state.ConnectionState.CONNECTED;
import static org.apache.curator.framework.state.ConnectionState.RECONNECTED;
import static org.apache.curator.utils.ZKPaths.makePath;
import static org.apache.zookeeper.CreateMode.EPHEMERAL;
import static org.apache.zookeeper.CreateMode.PERSISTENT;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;

import com.github.phantomthief.util.ThrowableFunction;

/**
 * @author w.vela
 * Created on 03/12/2016.
 */
public class ZkUtils {

    private static final Logger logger = getLogger(ZkUtils.class);

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
        } catch (NoNodeException e) {
            return null;
        } catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    public static void setToZk(CuratorFramework client, String path, byte[] value) {
        setToZk(client, path, value, PERSISTENT);
    }

    /**
     * use {@link #setToZk(CuratorFramework, String, byte[])}
     * or {@link #createEphemeralNode(CuratorFramework, String, byte[])}
     */
    @Deprecated
    public static void setToZk(CuratorFramework client, String path, byte[] value,
            CreateMode createMode) {
        checkNotNull(client);
        checkNotNull(path);
        checkNotNull(value);
        checkNotNull(createMode);
        int retryTimes = 0;
        while (retryTimes++ < 3) {
            try {
                client.setData().forPath(path, value);
                break;
            } catch (NoNodeException e) {
                try {
                    client.create().creatingParentsIfNeeded().withMode(createMode)
                            .forPath(path, value);
                    break;
                } catch (NodeExistsException retry) {
                    continue;
                } catch (Exception toThrow) {
                    throwIfUnchecked(toThrow);
                    throw new RuntimeException(toThrow);
                }
            } catch (Exception toThrow) {
                throwIfUnchecked(toThrow);
                throw new RuntimeException(toThrow);
            }
        }
    }

    public static EphemeralNode createEphemeralNode(CuratorFramework client, String path,
            byte[] value) throws NodeExistsException {
        checkNotNull(client);
        checkNotNull(path);
        checkNotNull(value);
        return new KeepEphemeralListener(client, path, value);
    }

    public static void removeFromZk(CuratorFramework client, String path) {
        removeFromZk(client, path, false);
    }

    public static void removeFromZk(CuratorFramework client, String path,
            boolean recruitDeletedChildren) {
        checkNotNull(client);
        checkNotNull(path);
        try {
            if (recruitDeletedChildren) {
                client.delete().deletingChildrenIfNeeded().forPath(path);
            } else {
                client.delete().forPath(path);
            }
        } catch (NoNodeException e) {
            logger.debug("no zookeeper path found:{}, ignore deleted.", path);
        } catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
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
            } catch (NoNodeException e) {
                byte[] newData = changeFunction.apply(null);
                try {
                    client.create().creatingParentsIfNeeded().forPath(path, newData);
                    if (logger.isDebugEnabled()) {
                        logger.debug("success create znode:{} -> {}", path,
                                Arrays.toString(newData));
                    }
                    return true;
                } catch (NodeExistsException ex) {
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

    public static Stream<ChildData> getAllChildrenWithData(CuratorFramework curator, String parentPath) {
        String parentPath0 = removeEnd(parentPath, "/");
        return getAllChildrenWithData0(curator, parentPath0);
    }

    private static Stream<ChildData> getAllChildrenWithData0(CuratorFramework curator, String parentPath) {
        try {
            List<String> children = curator.getChildren().forPath(parentPath);
            if (children.isEmpty()) {
                return empty();
            } else {
                Stream<ChildData> original = children.stream()
                        .map(child -> makePath(parentPath, child))
                        .map(path -> toChildData(curator, path))
                        .filter(Objects::nonNull);
                return concat(original,
                        children.stream()
                                .map(child -> makePath(parentPath, child))
                                .flatMap(path -> getAllChildrenWithData0(curator, path)));
            }
        } catch (NoNodeException e) {
            return empty();
        } catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    @Nullable
    private static ChildData toChildData(CuratorFramework curator, String path) {
        Stat stat = new Stat();
        try {
            byte[] bytes = curator.getData().storingStatIn(stat).forPath(path);
            return new ChildData(path, stat, bytes);
        } catch (NoNodeException e) {
            return null;
        } catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    public static Stream<String> getAllChildren(CuratorFramework curator, String parentPath) {
        String parentPath0 = removeEnd(parentPath, "/");
        return getAllChildren0(curator, parentPath0).map(p -> removeStart(p, parentPath0));
    }

    private static Stream<String> getAllChildren0(CuratorFramework curator, String parentPath) {
        try {
            List<String> children = curator.getChildren().forPath(parentPath);
            if (children.isEmpty()) {
                return children.stream();
            } else {
                Stream<String> original = children.stream()
                        .map(child -> makePath(parentPath, child));
                return concat(original,
                        children.stream()
                                .map(child -> makePath(parentPath, child))
                                .flatMap(path -> getAllChildren0(curator, path)));
            }
        } catch (NoNodeException e) {
            return empty();
        } catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    static class KeepEphemeralListener implements EphemeralNode, ConnectionStateListener {

        private static final long UNKNOWN_SESSION_ID = -1L;
        private final CuratorFramework originalClient;
        private final String path;
        private long lastSessionId;

        private volatile byte[] value;

        @GuardedBy("this")
        private volatile boolean closed;

        KeepEphemeralListener(CuratorFramework originalClient, String path, byte[] value)
                throws NodeExistsException {
            try {
                originalClient.create().creatingParentsIfNeeded().withMode(EPHEMERAL).forPath(path,
                        value);
            } catch (NodeExistsException e) {
                throw e;
            } catch (Exception e) {
                throwIfUnchecked(e);
                throw new RuntimeException(e);
            }
            this.originalClient = originalClient;
            this.path = path;
            this.value = value;
            this.originalClient.getConnectionStateListenable().addListener(this);
        }

        @Override
        public void close() throws Exception {
            synchronized (this) {
                closed = true;
                originalClient.getConnectionStateListenable().removeListener(this);
                try {
                    originalClient.delete().forPath(path);
                } catch (NoNodeException e) {
                    // ignore
                }
            }
        }

        @Override
        public void stateChanged(CuratorFramework client, ConnectionState newState) {
            synchronized (this) {
                if (closed) {
                    return;
                }

                long sessionId = UNKNOWN_SESSION_ID;
                try {
                    sessionId = client.getZookeeperClient().getZooKeeper().getSessionId();
                } catch (Exception e) {
                    logger.warn("Curator client state changed, but failed to get the related zk session instance.");
                }

                if (newState == CONNECTED) {
                    lastSessionId = sessionId;
                    logger.info("Curator zookeeper client instance initiated successfully, session id is " + sessionId);
                } else if (newState == RECONNECTED) {
                    try {
                        if (originalClient.checkExists().forPath(path) == null) {
                            logger.info("try recovery ephemeral node for:{}", path);
                            originalClient.create().creatingParentsIfNeeded().withMode(EPHEMERAL)
                                    .forPath(path, value);
                        }
                    } catch (NodeExistsException e) {
                        logger.warn("node exists when new state is RECONNECTED and try to recovery ephemeral node");
                        if (lastSessionId == sessionId && sessionId != UNKNOWN_SESSION_ID) {
                            logger.warn("Curator zookeeper connection recovered from connection lose, "
                                    + "reuse the old session " + Long.toHexString(sessionId));
                        } else {
                            logger.warn("New session created after old session lost, "
                                    + "old session " + Long.toHexString(lastSessionId)
                                    + ", new session " + Long.toHexString(sessionId));
                            lastSessionId = sessionId;
                            try {
                                originalClient.delete().forPath(path);
                                originalClient.create().creatingParentsIfNeeded().withMode(EPHEMERAL)
                                        .forPath(path, value);
                            } catch (Exception exception) {
                                logger.error("delete old ephemeral node and create new ephemeral node failed, ",
                                        exception.getMessage());
                            }
                        }
                    } catch (Exception e) {
                        logger.error("", e);
                    }
                }
            }
        }

        @Override
        public void updateValue(@Nonnull byte[] value) {
            synchronized (this) {
                if (closed) {
                    return;
                }
                this.value = checkNotNull(value);
                try {
                    originalClient.setData().forPath(path, value);
                } catch (NoNodeException e) {
                    try {
                        originalClient.create().creatingParentsIfNeeded().withMode(EPHEMERAL)
                                .forPath(path, value);
                    } catch (NodeExistsException e1) {
                        // ignore
                    } catch (Exception e1) {
                        throwIfUnchecked(e1);
                        throw new RuntimeException(e1);
                    }
                } catch (Exception e) {
                    throwIfUnchecked(e);
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
