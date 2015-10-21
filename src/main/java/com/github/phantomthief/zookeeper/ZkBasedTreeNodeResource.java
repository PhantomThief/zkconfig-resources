/**
 * 
 */
package com.github.phantomthief.zookeeper;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * @author w.vela
 */
public final class ZkBasedTreeNodeResource<T> implements Closeable {

    private static Logger logger = LoggerFactory.getLogger(ZkBasedTreeNodeResource.class);

    private final Function<Map<String, ChildData>, T> factory;
    private final Predicate<T> cleanup;
    private final long waitStopPeriod;
    private final BiConsumer<T, T> onResourceChange;
    private final Supplier<CuratorFramework> curatorFrameworkFactory;
    private final String path;
    private volatile TreeCache treeCache;

    private ZkBasedTreeNodeResource(Function<Map<String, ChildData>, T> factory,
            Supplier<CuratorFramework> curatorFrameworkFactory, String path, Predicate<T> cleanup,
            long waitStopPeriod, BiConsumer<T, T> onResourceChange) {
        this.factory = factory;
        this.cleanup = cleanup;
        this.path = path;
        this.waitStopPeriod = waitStopPeriod;
        this.curatorFrameworkFactory = curatorFrameworkFactory;
        this.onResourceChange = onResourceChange;
    }

    private TreeCache treeCache() {
        if (treeCache == null) {
            synchronized (ZkBasedTreeNodeResource.this) {
                if (treeCache == null) {
                    try {
                        CountDownLatch countDownLatch = new CountDownLatch(1);
                        TreeCache building = TreeCache
                                .newBuilder(curatorFrameworkFactory.get(), path) //
                                .setCacheData(true) //
                                .build();
                        building.getListenable().addListener((c, e) -> {
                            if (e.getType() == Type.INITIALIZED) {
                                countDownLatch.countDown();
                            }
                        });
                        building.start();
                        countDownLatch.await();
                        treeCache = building;
                    } catch (Throwable e) {
                        logger.error("Ops.", e);
                        throw Throwables.propagate(e);
                    }
                }
            }
        }
        return treeCache;
    }

    private volatile T resource;

    public T get() {
        if (resource == null) {
            synchronized (ZkBasedTreeNodeResource.this) {
                if (resource == null) {
                    TreeCache cache = treeCache();
                    resource = doFactory();
                    cache.getListenable().addListener((zk, event) -> {
                        T oldResource = null;
                        synchronized (ZkBasedTreeNodeResource.this) {
                            oldResource = resource;
                            resource = doFactory();
                            cleanup(resource, oldResource);
                        }
                    });
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
                                + "] cleanup thread-[%d]") //
                        .setUncaughtExceptionHandler(
                                (t, e) -> logger.error("fail to cleanup resource, path:{}, {}",
                                        path, oldResource.getClass().getSimpleName(), e)) //
                        .setPriority(Thread.MIN_PRIORITY) //
                        .setDaemon(true) //
                        .build() //
                        .newThread(() -> {
                            do {
                                if (waitStopPeriod > 0) {
                                    sleepUninterruptibly(waitStopPeriod, TimeUnit.MILLISECONDS);
                                }
                                if (cleanup.test(oldResource)) {
                                    break;
                                }
                            } while (true);
                            logger.info("successfully close old resource, path:{}, {}->{}", path,
                                    oldResource, currentResource);
                            if (onResourceChange != null) {
                                onResourceChange.accept(currentResource, oldResource);
                            }
                        }).start();
            }
        }
    }

    @Override
    public void close() throws IOException {
        synchronized (ZkBasedTreeNodeResource.this) {
            if (resource != null && cleanup != null) {
                cleanup.test(resource);
            }
            if (treeCache != null) {
                treeCache.close();
            }
        }
    }

    public static final class Builder<E> {

        private Function<Map<String, ChildData>, E> factory;
        private String path;
        private Supplier<CuratorFramework> curatorFrameworkFactory;
        private Predicate<E> cleanup;
        private long waitStopPeriod;
        private BiConsumer<E, E> onResourceChange;

        public Builder<E> path(String path) {
            this.path = path;
            return this;
        }

        public Builder<E> factory(Function<Map<String, ChildData>, E> factory) {
            this.factory = factory;
            return this;
        }

        public Builder<E> childDataFactory(Function<Collection<ChildData>, E> factory) {
            checkNotNull(factory);
            return factory(map -> factory.apply(map.values()));
        }

        public Builder<E> keysFactory(Function<Collection<String>, E> factory) {
            checkNotNull(factory);
            return factory(map -> factory.apply(map.keySet()));
        }

        public Builder<E> onResourceChange(BiConsumer<E, E> callback) {
            this.onResourceChange = callback;
            return this;
        }

        public Builder<E> curator(Supplier<CuratorFramework> curatorFactory) {
            this.curatorFrameworkFactory = curatorFactory;
            return this;
        }

        public Builder<E> curator(CuratorFramework curator) {
            this.curatorFrameworkFactory = () -> curator;
            return this;
        }

        public Builder<E> cleanup(Consumer<E> cleanup) {
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

        public Builder<E> cleanup(Predicate<E> cleanup) {
            this.cleanup = cleanup;
            return this;
        }

        public Builder<E> withWaitStopPeriod(long waitStopPeriod) {
            this.waitStopPeriod = waitStopPeriod;
            return this;
        }

        public ZkBasedTreeNodeResource<E> build() {
            ensure();
            return new ZkBasedTreeNodeResource<>(factory, curatorFrameworkFactory, path, cleanup,
                    waitStopPeriod, onResourceChange);
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
                            throw Throwables.propagate(e);
                        }
                    }
                });
            }
        }
    }

    private T doFactory() {
        Map<String, ChildData> map = new HashMap<>();
        generateFullTree(map, treeCache, path);
        return factory.apply(map);
    }

    private void generateFullTree(Map<String, ChildData> map, TreeCache cache, String rootPath) {
        Map<String, ChildData> thisMap = cache.getCurrentChildren(rootPath);
        thisMap.values().forEach(c -> map.put(StringUtils.removeStart(c.getPath(), path), c));
        thisMap.values().forEach(c -> generateFullTree(map, cache, c.getPath()));
    }

    public static final <T> Builder<T> newBuilder() {
        return new Builder<>();
    }
}
