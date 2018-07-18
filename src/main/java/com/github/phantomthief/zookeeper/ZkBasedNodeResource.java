package com.github.phantomthief.zookeeper;

import static com.github.phantomthief.util.MoreSuppliers.lazy;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Throwables.propagate;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.lang.Thread.MIN_PRIORITY;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;

import com.github.phantomthief.util.ThrowableBiConsumer;
import com.github.phantomthief.util.ThrowableBiFunction;
import com.github.phantomthief.util.ThrowableConsumer;
import com.github.phantomthief.util.ThrowableFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * @author w.vela
 */
public final class ZkBasedNodeResource<T> implements Closeable {

    private static final Logger logger = getLogger(ZkBasedNodeResource.class);

    private static final int UNKNOWN = 0;
    private static final int EXISTS = 1;
    private static final int NOT_EXISTS = 2;

    private final Object lock = new Object();

    private final ThrowableBiFunction<byte[], Stat, T, Exception> factory;
    private final ThrowableBiFunction<byte[], Stat, ListenableFuture<T>, Exception> refreshFactory;
    private final Predicate<T> cleanup;
    private final long waitStopPeriod;
    private final T emptyObject;
    private final BiConsumer<T, T> onResourceChange;
    private final Supplier<NodeCache> nodeCache;
    private final Runnable nodeCacheShutdown;
    private final BiConsumer<ChildData, Throwable> factoryFailedListener;

    @GuardedBy("lock")
    private volatile T resource;

    @GuardedBy("lock")
    private volatile boolean emptyLogged = false;

    @GuardedBy("lock")
    private volatile boolean closed = false;

    /**
     * {@link #UNKNOWN}, {@link #EXISTS} or {@link #NOT_EXISTS}
     */
    private volatile int zkNodeExists = UNKNOWN;

    private volatile boolean hasAddListener = false;
    private volatile Runnable nodeCacheRemoveListener;

    private ZkBasedNodeResource(Builder<T> builder) {
        this.factory = builder.factory;
        this.refreshFactory = builder.refreshFactory;
        this.cleanup = builder.cleanup;
        this.waitStopPeriod = builder.waitStopPeriod;
        this.emptyObject = builder.emptyObject;
        this.onResourceChange = builder.onResourceChange;
        this.nodeCacheShutdown = builder.nodeCacheShutdown;
        this.nodeCache = lazy(builder.cacheFactory);
        this.factoryFailedListener = (d, t) -> {
            for (ThrowableBiConsumer<ChildData, Throwable, ?> failedListener : builder.factoryFailedListeners) {
                try {
                    failedListener.accept(d, t);
                } catch (Throwable e) {
                    logger.error("", e);
                }
            }
        };
    }

    /**
     * use {@link #newGenericBuilder()} instead
     */
    @Deprecated
    public static Builder<Object> newBuilder() {
        return new Builder<>();
    }

    @CheckReturnValue
    public static <T> GenericZkBasedNodeBuilder<T> newGenericBuilder() {
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

    private static String zkConn(CuratorFramework zk) {
        String result = zk.getZookeeperClient().getCurrentConnectionString();
        if (StringUtils.isNotBlank(zk.getNamespace())) {
            result += "[" + zk.getNamespace() + "]";
        }
        return result;
    }

    public ZkNode<T> zkNode() {
        T t = get();
        return new ZkNode<>(t, zkNodeExists == EXISTS);
    }

    public T get() {
        checkClosed();
        if (resource == null) {
            if (zkNodeExists == NOT_EXISTS) { // for performance, short circuit it outside sync block.
                return emptyObject;
            }
            synchronized (lock) {
                checkClosed();
                if (resource == null) {
                    NodeCache cache = nodeCache.get();
                    tryAddListener(cache);
                    ChildData currentData = cache.getCurrentData();
                    if (currentData == null || currentData.getData() == null) {
                        zkNodeExists = NOT_EXISTS;
                        if (!emptyLogged) { // 只在刚开始一次或者几次打印这个log
                            logger.warn("found no zk path for:{}:{}, using empty data:{}",
                                    zkConn(cache.getClient()), path(cache), emptyObject);
                            emptyLogged = true;
                        }
                        return emptyObject;
                    }
                    zkNodeExists = EXISTS;
                    try {
                        resource = factory.apply(currentData.getData(), currentData.getStat());
                        if (onResourceChange != null) {
                            onResourceChange.accept(resource, emptyObject);
                        }
                    } catch (Exception e) {
                        factoryFailedListener.accept(currentData, e);
                        throwIfUnchecked(e);
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        return resource;
    }

    private void checkClosed() {
        if (closed) {
            throw new IllegalStateException("zkNode has been closed.");
        }
    }

    private void tryAddListener(NodeCache cache) {
        if (!hasAddListener) {
            NodeCacheListener nodeCacheListener = () -> {
                T oldResource;
                synchronized (lock) {
                    ChildData data = cache.getCurrentData();
                    oldResource = resource;
                    if (data != null && data.getData() != null) {
                        zkNodeExists = EXISTS;
                        ListenableFuture<T> future = refreshFactory.apply(data.getData(),
                                data.getStat());
                        addCallback(future, new FutureCallback<T>() {

                            @Override
                            public void onSuccess(@Nullable T result) {
                                resource = result;
                                cleanup(resource, oldResource, cache);
                            }

                            @Override
                            public void onFailure(Throwable t) {
                                factoryFailedListener.accept(data, t);
                                logger.error("", t);
                            }
                        }, directExecutor());
                    } else {
                        zkNodeExists = NOT_EXISTS;
                        resource = null;
                        emptyLogged = false;
                        cleanup(resource, oldResource, cache);
                    }
                }
            };
            cache.getListenable().addListener(nodeCacheListener);
            nodeCacheRemoveListener = () -> cache.getListenable().removeListener(nodeCacheListener);
            hasAddListener = true;
        }
    }

    private void cleanup(T currentResource, T oldResource, NodeCache nodeCache) {
        if (oldResource != null && oldResource != emptyObject) {
            if (currentResource == oldResource) {
                logger.warn(
                        "[BUG!!!!] should NOT occurred, old resource is same as current, path:{}, {}",
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
    public void close() {
        synchronized (lock) {
            if (nodeCacheShutdown != null) {
                nodeCacheShutdown.run();
            }
            if (nodeCacheRemoveListener != null) {
                nodeCacheRemoveListener.run();
            }
            if (resource != null && resource != emptyObject && cleanup != null) {
                cleanup.test(resource);
            }
            closed = true;
        }
    }

    public boolean isClosed() {
        return closed;
    }

    /**
     * use {@link #newGenericBuilder()} instead
     */
    @Deprecated
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static final class Builder<E> {

        private ThrowableBiFunction<byte[], Stat, E, Exception> factory;
        private ThrowableBiFunction<byte[], Stat, ListenableFuture<E>, Exception> refreshFactory;
        private Supplier<NodeCache> cacheFactory;
        private Predicate<E> cleanup;
        private long waitStopPeriod;
        private E emptyObject;
        private BiConsumer<E, E> onResourceChange;
        private Runnable nodeCacheShutdown;
        private ListeningExecutorService refreshExecutor;
        private List<ThrowableBiConsumer<ChildData, Throwable, Throwable>> factoryFailedListeners = new ArrayList<>();

        @CheckReturnValue
        public <E1> Builder<E1> addFactoryFailedListener(
                @Nonnull ThrowableConsumer<Throwable, Throwable> listener) {
            checkNotNull(listener);
            return addFactoryFailedListener((d, t) -> listener.accept(t));
        }

        @CheckReturnValue
        public <E1> Builder<E1> addFactoryFailedListener(
                @Nonnull ThrowableBiConsumer<ChildData, Throwable, Throwable> listener) {
            factoryFailedListeners.add(checkNotNull(listener));
            return (Builder<E1>) this;
        }

        /**
         * use {@link #withFactoryEx}
         */
        @Deprecated
        @CheckReturnValue
        public <E1> Builder<E1>
                withFactory(@Nonnull BiFunction<byte[], Stat, ? extends E1> factory) {
            return withFactoryEx(factory::apply);
        }

        @CheckReturnValue
        public <E1> Builder<E1> withFactoryEx(
                @Nonnull ThrowableBiFunction<byte[], Stat, ? extends E1, Exception> factory) {
            Builder<E1> thisBuilder = (Builder<E1>) this;
            thisBuilder.factory = (ThrowableBiFunction<byte[], Stat, E1, Exception>) factory;
            return thisBuilder;
        }

        @CheckReturnValue
        public <E1> Builder<E1> withRefreshStringFactory(
                @Nonnull ThrowableBiFunction<String, Stat, ? extends E1, Exception> factory) {
            return withRefreshStringFactory(null, factory);
        }

        @CheckReturnValue
        public <E1> Builder<E1> withRefreshStringFactory(
                @Nullable ListeningExecutorService executor,
                @Nonnull ThrowableBiFunction<String, Stat, ? extends E1, Exception> factory) {
            return withRefreshFactory(executor,
                    (b, s) -> factory.apply(b == null ? null : new String(b), s));
        }

        @CheckReturnValue
        public <E1> Builder<E1> withRefreshFactory(
                @Nonnull ThrowableBiFunction<byte[], Stat, ? extends E1, Exception> factory) {
            return withRefreshFactory(null, factory);
        }

        @CheckReturnValue
        public <E1> Builder<E1> withRefreshFactory(@Nullable ListeningExecutorService executor,
                @Nonnull ThrowableBiFunction<byte[], Stat, ? extends E1, Exception> factory) {
            Builder<E1> thisBuilder = (Builder<E1>) this;
            if (executor == null) {
                thisBuilder.refreshFactory = (b, s) -> immediateFuture(factory.apply(b, s));
            } else {
                thisBuilder.refreshFactory = (b, s) -> executor.submit(() -> factory.apply(b, s));
            }
            return thisBuilder;
        }

        @CheckReturnValue
        public <E1> Builder<E1> withAsyncRefreshStringFactory(
                @Nonnull ThrowableBiFunction<String, Stat, ListenableFuture<E1>, Exception> factory) {
            return withAsyncRefreshFactory(
                    (b, s) -> factory.apply(b == null ? null : new String(b), s));
        }

        @CheckReturnValue
        public <E1> Builder<E1> withAsyncRefreshFactory(
                @Nonnull ThrowableBiFunction<byte[], Stat, ListenableFuture<E1>, Exception> factory) {
            Builder<E1> thisBuilder = (Builder<E1>) this;
            thisBuilder.refreshFactory = factory;
            return thisBuilder;
        }

        @CheckReturnValue
        public <E1> Builder<E1> onResourceChange(BiConsumer<? super E1, ? super E1> callback) {
            Builder<E1> thisBuilder = (Builder<E1>) this;
            thisBuilder.onResourceChange = (BiConsumer<E1, E1>) callback;
            return thisBuilder;
        }

        /**
         * use {@link #withFactoryEx}
         */
        @Deprecated
        @CheckReturnValue
        public <E1> Builder<E1> withFactory(@Nonnull Function<byte[], ? extends E1> factory) {
            return withFactoryEx(factory::apply);
        }

        @CheckReturnValue
        public <E1> Builder<E1>
                withFactoryEx(@Nonnull ThrowableFunction<byte[], ? extends E1, Exception> factory) {
            return withFactoryEx((b, s) -> factory.apply(b));
        }

        @CheckReturnValue
        public <E1> Builder<E1> withRefreshStringFactory(
                @Nonnull ThrowableFunction<String, ? extends E1, Exception> factory) {
            return withRefreshStringFactory(null, factory);
        }

        @CheckReturnValue
        public <E1> Builder<E1> withRefreshStringFactory(
                @Nullable ListeningExecutorService executor,
                @Nonnull ThrowableFunction<String, ? extends E1, Exception> factory) {
            return withRefreshStringFactory(executor, (b, s) -> factory.apply(b));
        }

        @CheckReturnValue
        public <E1> Builder<E1> withRefreshFactory(
                @Nonnull ThrowableFunction<byte[], ? extends E1, Exception> factory) {
            return withRefreshFactory(null, factory);
        }

        @CheckReturnValue
        public <E1> Builder<E1> withRefreshFactory(@Nullable ListeningExecutorService executor,
                @Nonnull ThrowableFunction<byte[], ? extends E1, Exception> factory) {
            return withRefreshFactory(executor, (b, s) -> factory.apply(b));
        }

        @CheckReturnValue
        public <E1> Builder<E1> withAsyncRefreshStringFactory(
                ThrowableFunction<String, ListenableFuture<E1>, Exception> factory) {
            return withAsyncRefreshStringFactory((b, s) -> factory.apply(b));
        }

        @CheckReturnValue
        public <E1> Builder<E1> withAsyncRefreshFactory(
                ThrowableFunction<byte[], ListenableFuture<E1>, Exception> factory) {
            return withAsyncRefreshFactory((b, s) -> factory.apply(b));
        }

        @CheckReturnValue
        public <E1> Builder<E1> asyncRefresh(@Nonnull ListeningExecutorService executor) {
            Builder<E1> thisBuilder = (Builder<E1>) this;
            thisBuilder.refreshExecutor = checkNotNull(executor);
            return thisBuilder;
        }

        /**
         * use {@link #withStringFactoryEx}
         */
        @Deprecated
        @CheckReturnValue
        public <E1> Builder<E1> withStringFactory(BiFunction<String, Stat, ? extends E1> factory) {
            return withStringFactoryEx(factory::apply);
        }

        @CheckReturnValue
        public <E1> Builder<E1> withStringFactoryEx(
                ThrowableBiFunction<String, Stat, ? extends E1, Exception> factory) {
            return withFactoryEx((b, s) -> factory.apply(b == null ? null : new String(b), s));
        }

        /**
         * use {@link #withStringFactoryEx}
         */
        @Deprecated
        @CheckReturnValue
        public <E1> Builder<E1> withStringFactory(Function<String, ? extends E1> factory) {
            return withStringFactoryEx(factory::apply);
        }

        @CheckReturnValue
        public <E1> Builder<E1>
                withStringFactoryEx(ThrowableFunction<String, ? extends E1, Exception> factory) {
            return withStringFactoryEx((b, s) -> factory.apply(b));
        }

        @CheckReturnValue
        public Builder<E> withCacheFactory(Supplier<NodeCache> cacheFactory) {
            this.cacheFactory = cacheFactory;
            return this;
        }

        @CheckReturnValue
        public Builder<E> withCacheFactory(String path, CuratorFramework curator) {
            return withCacheFactory(path, () -> curator);
        }

        @CheckReturnValue
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
                    this.nodeCacheShutdown = () -> {
                        try {
                            buildingCache.close();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    };
                    return buildingCache;
                } catch (Throwable e) {
                    throwIfUnchecked(e);
                    throw new RuntimeException(e);
                }
            };
            return this;
        }

        /**
         * using {@link #withCleanupConsumer(ThrowableConsumer)}
         */
        @Deprecated
        @CheckReturnValue
        public <E1> Builder<E1> withCleanup(ThrowableConsumer<? super E1, Throwable> cleanup) {
            return withCleanupConsumer(cleanup);
        }

        @CheckReturnValue
        public <E1> Builder<E1>
                withCleanupConsumer(ThrowableConsumer<? super E1, Throwable> cleanup) {
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
        @CheckReturnValue
        public <E1> Builder<E1> withCleanup(Predicate<? super E1> cleanup) {
            return withCleanupPredicate(cleanup);
        }

        @CheckReturnValue
        public <E1> Builder<E1> withCleanupPredicate(Predicate<? super E1> cleanup) {
            Builder<E1> thisBuilder = (Builder<E1>) this;
            thisBuilder.cleanup = (Predicate<E1>) cleanup;
            return thisBuilder;
        }

        @CheckReturnValue
        public Builder<E> withWaitStopPeriod(long waitStopPeriod) {
            this.waitStopPeriod = waitStopPeriod;
            return this;
        }

        @CheckReturnValue
        public <E1> Builder<E1> withEmptyObject(E1 emptyObject) {
            Builder<E1> thisBuilder = (Builder<E1>) this;
            thisBuilder.emptyObject = emptyObject;
            return thisBuilder;
        }

        public <E1> ZkBasedNodeResource<E1> build() {
            ensure();
            return new ZkBasedNodeResource(this);
        }

        private void ensure() {
            checkNotNull(factory);
            checkNotNull(cacheFactory);
            if (refreshFactory == null) {
                if (refreshExecutor != null) {
                    refreshFactory = (bs, stat) -> refreshExecutor
                            .submit(() -> factory.apply(bs, stat));
                } else {
                    refreshFactory = (bs, stat) -> immediateFuture(factory.apply(bs, stat));
                }
            }

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
