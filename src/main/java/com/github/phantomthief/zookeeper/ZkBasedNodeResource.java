/**
 * 
 */
package com.github.phantomthief.zookeeper;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Suppliers.memoize;
import static com.google.common.base.Throwables.propagate;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.lang.Thread.MIN_PRIORITY;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * @author w.vela
 */
public final class ZkBasedNodeResource<T> implements Closeable {

    private static Logger logger = getLogger(ZkBasedNodeResource.class);

    private final BiFunction<byte[], Stat, T> factory;
    private final Predicate<T> cleanup;
    private final long waitStopPeriod;
    private final T emptyObject;
    private final BiConsumer<T, T> onResourceChange;
    private final com.google.common.base.Supplier<NodeCache> nodeCache;
    private volatile T resource;
    private volatile boolean emptyLogged = false;

    private ZkBasedNodeResource(BiFunction<byte[], Stat, T> factory,
            Supplier<NodeCache> cacheFactory, Predicate<T> cleanup, long waitStopPeriod,
            BiConsumer<T, T> onResourceChange, T emptyObject) {
        this.factory = factory;
        this.cleanup = cleanup;
        this.waitStopPeriod = waitStopPeriod;
        this.emptyObject = emptyObject;
        this.onResourceChange = onResourceChange;
        this.nodeCache = memoize(cacheFactory::get);
    }

    public static Builder<Object> newBuilder() {
        return new Builder<>();
    }

    public static <T> GenericZkBasedNodeBuilder<T> newGenericBuilder(){
        return new GenericZkBasedNodeBuilder<>(newBuilder());
    }

    private static String path(NodeCache nodeCache) {
        try {
            if (nodeCache == null) {
                return "n/a";
            }
            Field f = NodeCache.class.getDeclaredField("path");
            f.setAccessible(true);
            return (String) f.get(nodeCache);
        } catch (Throwable e) {
            logger.error("Ops.fail to get path from node:{}, exception:{}", nodeCache,
                    e.toString());
            return null;
        }
    }

    public T get() {
        if (resource == null) {
            synchronized (ZkBasedNodeResource.this) {
                if (resource == null) {
                    NodeCache cache = nodeCache.get();
                    ChildData currentData = cache.getCurrentData();
                    if (currentData == null || currentData.getData() == null) {
                        if (!emptyLogged) { // 只在刚开始一次或者几次打印这个log
                            logger.warn("found no zk path for:{}, using empty data:{}", path(cache),
                                    emptyObject);
                            emptyLogged = true;
                        }
                        return emptyObject;
                    }
                    resource = factory.apply(currentData.getData(), currentData.getStat());
                    cache.getListenable().addListener(() -> {
                        T oldResource = null;
                        synchronized (ZkBasedNodeResource.this) {
                            ChildData data = cache.getCurrentData();
                            oldResource = resource;
                            if (data != null && data.getData() != null) {
                                resource = factory.apply(data.getData(), data.getStat());
                            } else {
                                resource = null;
                                emptyLogged = false;
                            }
                            cleanup(resource, oldResource, cache);
                        }
                    });
                }
            }
        }
        return resource;
    }

    private void cleanup(T currentResource, T oldResource, NodeCache nodeCache) {
        if (oldResource != null && oldResource != emptyObject) {
            if (currentResource == oldResource) {
                logger.warn(
                        "[BUG!!!!] should NOT occured, old resource is same as current, path:{}, {}",
                        path(nodeCache), oldResource);
            } else {
                new ThreadFactoryBuilder() //
                        .setNameFormat("old [" + oldResource.getClass().getSimpleName()
                                + "] cleanup thread-[%d]")
                        .setUncaughtExceptionHandler(
                                (t, e) -> logger.error("fail to cleanup resource, path:{}, {}",
                                        path(nodeCache), oldResource.getClass().getSimpleName(), e)) //
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
                            logger.info("successfully close old resource, path:{}, {}->{}",
                                    path(nodeCache), oldResource, currentResource);
                            if (onResourceChange != null) {
                                onResourceChange.accept(currentResource, oldResource);
                            }
                        }).start();
            }
        }
    }

    @Override
    public void close() throws IOException {
        synchronized (ZkBasedNodeResource.this) {
            if (resource != null && resource != emptyObject && cleanup != null) {
                cleanup.test(resource);
            }
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static final class Builder<E> {

        private BiFunction<byte[], Stat, E> factory;
        private Supplier<NodeCache> cacheFactory;
        private Predicate<E> cleanup;
        private long waitStopPeriod;
        private E emptyObject;
        private BiConsumer<E, E> onResourceChange;

        public <E1> Builder<E1> withFactory(BiFunction<byte[], Stat, ? extends E1> factory) {
            Builder<E1> thisBuilder = (Builder<E1>) this;
            thisBuilder.factory = (BiFunction<byte[], Stat, E1>) factory;
            return thisBuilder;
        }

        public <E1> Builder<E1> onResourceChange(BiConsumer<? super E1, ? super E1> callback) {
            Builder<E1> thisBuilder = (Builder<E1>) this;
            thisBuilder.onResourceChange = (BiConsumer<E1, E1>) callback;
            return thisBuilder;
        }

        public <E1> Builder<E1> withFactory(Function<byte[], ? extends E1> factory) {
            Builder<E1> thisBuilder = (Builder<E1>) this;
            thisBuilder.factory = (data, stat) -> factory.apply(data);
            return thisBuilder;
        }

        public <E1> Builder<E1> withStringFactory(BiFunction<String, Stat, ? extends E1> factory) {
            Builder<E1> thisBuilder = (Builder<E1>) this;
            thisBuilder.factory = (data, stat) -> factory
                    .apply(data == null ? null : new String(data), stat);
            return thisBuilder;
        }

        public <E1> Builder<E1> withStringFactory(Function<String, ? extends E1> factory) {
            Builder<E1> thisBuilder = (Builder<E1>) this;
            thisBuilder.factory = (data, stat) -> factory
                    .apply(data == null ? null : new String(data));
            return thisBuilder;
        }

        public Builder<E> withCacheFactory(Supplier<NodeCache> cacheFactory) {
            this.cacheFactory = cacheFactory;
            return this;
        }

        public Builder<E> withCacheFactory(String path, CuratorFramework curator) {
            return withCacheFactory(path, () -> curator);
        }

        public Builder<E> withCacheFactory(String path, Supplier<CuratorFramework> curatorFactory) {
            this.cacheFactory = () -> {
                CuratorFramework thisClient = curatorFactory.get();
                if (thisClient.getState() != CuratorFrameworkState.STARTED) {
                    thisClient.start();
                }
                NodeCache buildingCache = new NodeCache(thisClient, path);
                try {
                    buildingCache.start();
                    buildingCache.rebuild();
                    return buildingCache;
                } catch (Throwable e) {
                    throw propagate(e);
                }
            };
            return this;
        }

        /**
         * using {@link #withCleanupConsumer(Consumer)}
         */
        @Deprecated
        public <E1> Builder<E1> withCleanup(Consumer<? super E1> cleanup) {
            return withCleanupConsumer(cleanup);
        }

        public <E1> Builder<E1> withCleanupConsumer(Consumer<? super E1> cleanup) {
            Builder<E1> thisBuilder = (Builder<E1>) this;
            thisBuilder.cleanup = t -> {
                try {
                    cleanup.accept(t);
                    return true;
                } catch (Throwable e) {
                    logger.error("Ops. fail to close, path:{}", t, e);
                    return false;
                }
            };
            return thisBuilder;
        }

        /**
         * using {@link #withCleanupPredicate(Predicate)}
         */
        @Deprecated
        public <E1> Builder<E1> withCleanup(Predicate<? super E1> cleanup) {
            return withCleanupPredicate(cleanup);
        }

        public <E1> Builder<E1> withCleanupPredicate(Predicate<? super E1> cleanup) {
            Builder<E1> thisBuilder = (Builder<E1>) this;
            thisBuilder.cleanup = (Predicate<E1>) cleanup;
            return thisBuilder;
        }

        public Builder<E> withWaitStopPeriod(long waitStopPeriod) {
            this.waitStopPeriod = waitStopPeriod;
            return this;
        }

        public <E1> Builder<E1> withEmptyObject(E1 emptyObject) {
            Builder<E1> thisBuilder = (Builder<E1>) this;
            thisBuilder.emptyObject = emptyObject;
            return thisBuilder;
        }

        public <E1> ZkBasedNodeResource<E1> build() {
            ensure();
            return new ZkBasedNodeResource(factory, cacheFactory, cleanup, waitStopPeriod,
                    onResourceChange, emptyObject);
        }

        private void ensure() {
            checkNotNull(factory);
            checkNotNull(cacheFactory);

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
                withCleanup(t -> {
                    if (t instanceof Closeable) {
                        try {
                            ((Closeable) t).close();
                        } catch (Throwable e) {
                            throw propagate(e);
                        }
                    }
                });
            }
        }
    }
}
