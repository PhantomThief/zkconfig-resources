package com.github.phantomthief.zookeeper;

import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.zookeeper.data.Stat;

import com.github.phantomthief.zookeeper.ZkBasedNodeResource.Builder;

/**
 * @author w.vela
 * Created on 16/5/13.
 */
public class GenericZkBasedNodeBuilder<T> {

    private final Builder<Object> builder;

    GenericZkBasedNodeBuilder(Builder<Object> builder) {
        this.builder = builder;
    }

    public GenericZkBasedNodeBuilder<T> withFactory(BiFunction<byte[], Stat, ? extends T> factory) {
        builder.withFactory(factory);
        return this;
    }

    public GenericZkBasedNodeBuilder<T>
            onResourceChange(BiConsumer<? super T, ? super T> callback) {
        builder.onResourceChange(callback);
        return this;
    }

    public GenericZkBasedNodeBuilder<T> withFactory(Function<byte[], ? extends T> factory) {
        builder.withFactory(factory);
        return this;
    }

    public GenericZkBasedNodeBuilder<T>
            withStringFactory(BiFunction<String, Stat, ? extends T> factory) {
        builder.withStringFactory(factory);
        return this;
    }

    public GenericZkBasedNodeBuilder<T> withStringFactory(Function<String, ? extends T> factory) {
        builder.withStringFactory(factory);
        return this;
    }

    public GenericZkBasedNodeBuilder<T> withCacheFactory(Supplier<NodeCache> cacheFactory) {
        builder.withCacheFactory(cacheFactory);
        return this;
    }

    public GenericZkBasedNodeBuilder<T> withCacheFactory(String path, CuratorFramework curator) {
        builder.withCacheFactory(path, curator);
        return this;
    }

    public GenericZkBasedNodeBuilder<T> withCacheFactory(String path,
            Supplier<CuratorFramework> curatorFactory) {
        builder.withCacheFactory(path, curatorFactory);
        return this;
    }

    public GenericZkBasedNodeBuilder<T> withCleanupConsumer(Consumer<? super T> cleanup) {
        builder.withCleanupConsumer(cleanup);
        return this;
    }

    public GenericZkBasedNodeBuilder<T> withCleanupPredicate(Predicate<? super T> cleanup) {
        builder.withCleanupPredicate(cleanup);
        return this;
    }

    public GenericZkBasedNodeBuilder<T> withWaitStopPeriod(long waitStopPeriod) {
        builder.withWaitStopPeriod(waitStopPeriod);
        return this;
    }

    public GenericZkBasedNodeBuilder<T> withEmptyObject(T emptyObject) {
        builder.withEmptyObject(emptyObject);
        return this;
    }

    public ZkBasedNodeResource<T> build() {
        return builder.build();
    }
}
