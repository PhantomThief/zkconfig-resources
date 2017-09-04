/**
 * 
 */
package com.github.phantomthief.zookeeper;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.lang.Thread.MIN_PRIORITY;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.commons.lang3.StringUtils.removeStart;
import static org.apache.curator.framework.recipes.cache.TreeCacheEvent.Type.CONNECTION_LOST;
import static org.apache.curator.framework.recipes.cache.TreeCacheEvent.Type.CONNECTION_SUSPENDED;
import static org.apache.curator.framework.recipes.cache.TreeCacheEvent.Type.INITIALIZED;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import javax.annotation.CheckReturnValue;
import javax.annotation.concurrent.GuardedBy;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.slf4j.Logger;

import com.github.phantomthief.util.ThrowableConsumer;
import com.github.phantomthief.util.ThrowableFunction;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * @author w.vela
 */
public final class ZkBasedTreeNodeResource<T> implements Closeable {

    private static Logger logger = getLogger(ZkBasedTreeNodeResource.class);

    private final Object lock = new Object();

    private final ThrowableFunction<Map<String, ChildData>, T, Exception> factory;
    private final Predicate<T> cleanup;
    private final long waitStopPeriod;
    private final BiConsumer<T, T> onResourceChange;
    private final Supplier<CuratorFramework> curatorFrameworkFactory;
    private final String path;

    @GuardedBy("lock")
    private volatile TreeCache treeCache;

    @GuardedBy("lock")
    private volatile T resource;

    @GuardedBy("lock")
    private volatile boolean closed;

    private volatile boolean hasAddListener = false;

    private ZkBasedTreeNodeResource(Builder<T> builder) {
        this.factory = builder.factory;
        this.cleanup = builder.cleanup;
        this.path = builder.path;
        this.waitStopPeriod = builder.waitStopPeriod;
        this.curatorFrameworkFactory = builder.curatorFrameworkFactory;
        this.onResourceChange = builder.onResourceChange;
    }

    public static <T> Builder<T> newBuilder() {
        return new Builder<>();
    }

    private TreeCache treeCache() {
        if (treeCache == null) {
            synchronized (lock) {
                if (treeCache == null) {
                    try {
                        CountDownLatch countDownLatch = new CountDownLatch(1);
                        TreeCache building = TreeCache
                                .newBuilder(curatorFrameworkFactory.get(), path) //
                                .setCacheData(true) //
                                .build();
                        building.getListenable().addListener((c, e) -> {
                            if (e.getType() == INITIALIZED) {
                                countDownLatch.countDown();
                            }
                        });
                        building.start();
                        countDownLatch.await();
                        treeCache = building;
                    } catch (Throwable e) {
                        throwIfUnchecked(e);
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        return treeCache;
    }

    public T get() {
        if (closed) {
            throw new IllegalStateException("zkNode has been closed.");
        }
        if (resource == null) {
            synchronized (lock) {
                if (closed) {
                    throw new IllegalStateException("zkNode has been closed.");
                }
                if (resource == null) {
                    TreeCache cache = treeCache();
                    try {
                        resource = doFactory();
                        if (onResourceChange != null) {
                            onResourceChange.accept(resource, null);
                        }
                    } catch (Exception e) {
                        throwIfUnchecked(e);
                        throw new RuntimeException(e);
                    }
                    if (!hasAddListener) {
                        cache.getListenable().addListener((zk, event) -> {
                            if (event.getType() == CONNECTION_SUSPENDED
                                    || event.getType() == CONNECTION_LOST) {
                                logger.info("ignore event:{} for tree node:{}", event.getType(),
                                        path);
                                return;
                            }
                            T oldResource;
                            synchronized (lock) {
                                oldResource = resource;
                                resource = doFactory();
                                cleanup(resource, oldResource);
                            }
                        });
                        hasAddListener = true;
                    }
                }
            }
        }
        return resource;
    }

    private void cleanup(T currentResource, T oldResource) {
        if (oldResource != null) {
            if (currentResource == oldResource) {
                logger.warn(
                        "[BUG!!!!] should NOT occured, old resource is same as current, path:{}, {}",
                        path, oldResource);
            } else {
                new ThreadFactoryBuilder() //
                        .setNameFormat("old [" + oldResource.getClass().getSimpleName()
                                + "] cleanup thread-[%d]")
                        .setUncaughtExceptionHandler(
                                (t, e) -> logger.error("fail to cleanup resource, path:{}, {}",
                                        path, oldResource.getClass().getSimpleName(), e)) //
                        .setPriority(MIN_PRIORITY) //
                        .setDaemon(true) //
                        .build() //
                        .newThread(() -> {
                            do {
                                if (waitStopPeriod > 0) {
                                    sleepUninterruptibly(waitStopPeriod, MILLISECONDS);
                                }
                                if (cleanup.test(oldResource)) {
                                    break;
                                }
                            } while (true);
                            if (onResourceChange != null) {
                                onResourceChange.accept(currentResource, oldResource);
                            }
                        }).start();
                return;
            }
        }
        if (onResourceChange != null) {
            onResourceChange.accept(currentResource, oldResource);
        }
    }

    @Override
    public void close() throws IOException {
        synchronized (lock) {
            if (resource != null && cleanup != null) {
                cleanup.test(resource);
            }
            if (treeCache != null) {
                treeCache.close();
            }
            closed = true;
        }
    }

    public boolean isClosed() {
        return closed;
    }

    private T doFactory() throws Exception {
        Map<String, ChildData> map = new HashMap<>();
        generateFullTree(map, treeCache, path);
        return factory.apply(map);
    }

    private void generateFullTree(Map<String, ChildData> map, TreeCache cache, String rootPath) {
        Map<String, ChildData> thisMap = cache.getCurrentChildren(rootPath);
        if (thisMap != null) {
            thisMap.values().forEach(c -> map.put(removeStart(c.getPath(), path), c));
            thisMap.values().forEach(c -> generateFullTree(map, cache, c.getPath()));
        }
    }

    public static final class Builder<E> {

        private ThrowableFunction<Map<String, ChildData>, E, Exception> factory;
        private String path;
        private Supplier<CuratorFramework> curatorFrameworkFactory;
        private Predicate<E> cleanup;
        private long waitStopPeriod;
        private BiConsumer<E, E> onResourceChange;

        @CheckReturnValue
        public Builder<E> path(String path) {
            this.path = path;
            return this;
        }

        /**
         * use {@link #factoryEx}
         */
        @Deprecated
        @CheckReturnValue
        public Builder<E> factory(Function<Map<String, ChildData>, E> factory) {
            return factoryEx(factory::apply);
        }

        @CheckReturnValue
        public Builder<E>
                factoryEx(ThrowableFunction<Map<String, ChildData>, E, Exception> factory) {
            this.factory = factory;
            return this;
        }

        /**
         * use {@link #childDataFactoryEx}
         */
        @CheckReturnValue
        @Deprecated
        public Builder<E> childDataFactory(Function<Collection<ChildData>, E> factory) {
            return childDataFactoryEx(factory::apply);
        }

        @CheckReturnValue
        public Builder<E>
                childDataFactoryEx(ThrowableFunction<Collection<ChildData>, E, Exception> factory) {
            checkNotNull(factory);
            return factoryEx(map -> factory.apply(map.values()));
        }

        /**
         * use {@link #keysFactoryEx}
         */
        @Deprecated
        @CheckReturnValue
        public Builder<E> keysFactory(Function<Collection<String>, E> factory) {
            return keysFactoryEx(factory::apply);
        }

        @CheckReturnValue
        public Builder<E>
                keysFactoryEx(ThrowableFunction<Collection<String>, E, Exception> factory) {
            checkNotNull(factory);
            return factoryEx(map -> factory.apply(map.keySet()));
        }

        @CheckReturnValue
        public Builder<E> onResourceChange(BiConsumer<E, E> callback) {
            this.onResourceChange = callback;
            return this;
        }

        @CheckReturnValue
        public Builder<E> curator(Supplier<CuratorFramework> curatorFactory) {
            this.curatorFrameworkFactory = curatorFactory;
            return this;
        }

        @CheckReturnValue
        public Builder<E> curator(CuratorFramework curator) {
            this.curatorFrameworkFactory = () -> curator;
            return this;
        }

        @CheckReturnValue
        public Builder<E> cleanup(ThrowableConsumer<E, Throwable> cleanup) {
            this.cleanup = t -> {
                try {
                    cleanup.accept(t);
                    return true;
                } catch (Throwable e) {
                    logger.error("Ops. fail to close, path:{}", t, e);
                    return false;
                }
            };
            return this;
        }

        @CheckReturnValue
        public Builder<E> cleanup(Predicate<E> cleanup) {
            this.cleanup = cleanup;
            return this;
        }

        @CheckReturnValue
        public Builder<E> withWaitStopPeriod(long waitStopPeriod) {
            this.waitStopPeriod = waitStopPeriod;
            return this;
        }

        public ZkBasedTreeNodeResource<E> build() {
            ensure();
            return new ZkBasedTreeNodeResource<>(this);
        }

        private void ensure() {
            checkNotNull(factory);
            checkNotNull(curatorFrameworkFactory);

            if (onResourceChange != null) {
                BiConsumer<E, E> temp = onResourceChange;
                onResourceChange = (t, u) -> { // safe wrapper
                    try {
                        temp.accept(t, u);
                    } catch (Throwable e) {
                        logger.error("Ops.", e);
                    }
                };
            }

            if (cleanup == null) {
                cleanup(t -> {
                    if (t instanceof Closeable) {
                        try {
                            ((Closeable) t).close();
                        } catch (Throwable e) {
                            throwIfUnchecked(e);
                            throw new RuntimeException(e);
                        }
                    }
                });
            }
        }
    }
}
