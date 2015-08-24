/**
 * 
 */
package com.github.phantomthief.zookeeper;

import java.io.Closeable;
import java.io.IOException;
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
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * @author w.vela
 */
public final class ZkBasedNodeResource<T> implements Closeable {

    private static Logger logger = LoggerFactory.getLogger(ZkBasedNodeResource.class);

    private final BiFunction<byte[], Stat, T> factory;
    private final Supplier<NodeCache> cacheFactory;
    private final Predicate<T> cleanup;
    private final long waitStopPeriod;
    private final T emptyObject;

    /**
     * @param factory
     * @param cacheFactory
     * @param cleanup
     * @param nodeChangeListener
     * @param waitStopPeriod
     * @param emptyObject
     */
    private ZkBasedNodeResource(BiFunction<byte[], Stat, T> factory,
            Supplier<NodeCache> cacheFactory, Predicate<T> cleanup, long waitStopPeriod,
            T emptyObject) {
        this.factory = factory;
        this.cacheFactory = cacheFactory;
        this.cleanup = cleanup;
        this.waitStopPeriod = waitStopPeriod;
        this.emptyObject = emptyObject;
    }

    private volatile T resource;

    public T get() {
        if (resource == null) {
            synchronized (ZkBasedNodeResource.this) {
                if (resource == null) {
                    NodeCache cache = cacheFactory.get();
                    ChildData currentData = cache.getCurrentData();
                    if (currentData == null || currentData.getData() == null) {
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
                                resource = emptyObject;
                            }
                        }
                        cleanup(oldResource);
                    });
                }
            }
        }
        return resource;
    }

    /**
     * @param oldResource
     */
    private void cleanup(T oldResource) {
        if (oldResource != null && oldResource != emptyObject) {
            if (cleanup == null) {
                return;
            }
            new ThreadFactoryBuilder() //
                    .setNameFormat("old [" + oldResource.getClass().getSimpleName()
                            + "] cleanup thread-[%d]") //
                    .setUncaughtExceptionHandler(
                            (t, e) -> logger.error("fail to cleanup resource:{}",
                                    oldResource.getClass().getSimpleName(), e)) //
                    .setPriority(Thread.MIN_PRIORITY) //
                    .setDaemon(true) //
                    .build() //
                    .newThread(() -> {
                        do {
                            if (waitStopPeriod > 0) {
                                try {
                                    Thread.sleep(waitStopPeriod);
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }
                            }
                            if (cleanup.test(oldResource)) {
                                break;
                            }
                        } while (true);
                        logger.info("successfully close old resource:{}", oldResource);
                    }) //
                    .start();;
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

    public static final class Builder<E> {

        private BiFunction<byte[], Stat, E> factory;
        private Supplier<NodeCache> cacheFactory;
        private Predicate<E> cleanup;
        private long waitStopPeriod;
        private E emptyObject;

        public Builder<E> withFactory(BiFunction<byte[], Stat, E> factory) {
            this.factory = factory;
            return this;
        }

        public Builder<E> withFactory(Function<byte[], E> factory) {
            this.factory = (data, stat) -> factory.apply(data);
            return this;
        }

        public Builder<E> withStringFactory(BiFunction<String, Stat, E> factory) {
            this.factory = (data, stat) -> factory.apply(data == null ? null : new String(data),
                    stat);
            return this;
        }

        public Builder<E> withStringFactory(Function<String, E> factory) {
            this.factory = (data, stat) -> factory.apply(data == null ? null : new String(data));
            return this;
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
                    throw Throwables.propagate(e);
                }
            };
            return this;
        }

        public Builder<E> withCleanup(Consumer<E> cleanup) {
            this.cleanup = t -> {
                try {
                    cleanup.accept(t);
                    return true;
                } catch (Throwable e) {
                    logger.error("Ops. fail to close:{}", t, e);
                    return false;
                }
            };
            return this;
        }

        public Builder<E> withCleanup(Predicate<E> cleanup) {
            this.cleanup = cleanup;
            return this;
        }

        public Builder<E> withWaitStopPeriod(long waitStopPeriod) {
            this.waitStopPeriod = waitStopPeriod;
            return this;
        }

        public Builder<E> withEmptyObject(E emptyObject) {
            this.emptyObject = emptyObject;
            return this;
        }

        public ZkBasedNodeResource<E> build() {
            ensure();
            return new ZkBasedNodeResource<>(factory, cacheFactory, cleanup, waitStopPeriod,
                    emptyObject);
        }

        private void ensure() {
            if (factory == null) {
                throw new NullPointerException("factory is null.");
            }
            if (cacheFactory == null) {
                throw new NullPointerException("cache factory is null.");
            }
            if (cleanup == null) {
                withCleanup(t -> {
                    if (t instanceof Closeable) {
                        try {
                            ((Closeable) t).close();
                        } catch (Throwable e) {
                            throw Throwables.propagate(e);
                        }
                    }
                });
            }
        }
    }

    public static final <T> Builder<T> newBuilder() {
        return new Builder<>();
    }
}
