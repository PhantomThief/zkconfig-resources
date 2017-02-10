package com.github.phantomthief.zookeeper;

import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.zookeeper.data.Stat;

import com.github.phantomthief.util.ThrowableBiFunction;
import com.github.phantomthief.util.ThrowableConsumer;
import com.github.phantomthief.util.ThrowableFunction;
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

    /**
     * use {@link #withFactoryEx}
     */
    @Deprecated
    public GenericZkBasedNodeBuilder<T> withFactory(BiFunction<byte[], Stat, ? extends T> factory) {
        return withFactoryEx(factory::apply);
    }

    public GenericZkBasedNodeBuilder<T>
            withFactoryEx(ThrowableBiFunction<byte[], Stat, ? extends T, Exception> factory) {
        builder.withFactoryEx(factory);
        return this;
    }

    public GenericZkBasedNodeBuilder<T>
            onResourceChange(BiConsumer<? super T, ? super T> callback) {
        builder.onResourceChange(callback);
        return this;
    }

    /**
     * use {@link #withFactoryEx}
     */
    @Deprecated
    public GenericZkBasedNodeBuilder<T> withFactory(Function<byte[], ? extends T> factory) {
        return withFactoryEx(factory::apply);
    }

    public GenericZkBasedNodeBuilder<T>
            withFactoryEx(ThrowableFunction<byte[], ? extends T, Exception> factory) {
        return withFactoryEx((b, s) -> factory.apply(b));
    }

    /**
     * use {@link #withStringFactoryEx}
     */
    @Deprecated
    public GenericZkBasedNodeBuilder<T>
            withStringFactory(BiFunction<String, Stat, ? extends T> factory) {
        return withStringFactoryEx(factory::apply);
    }

    public GenericZkBasedNodeBuilder<T>
            withStringFactoryEx(ThrowableBiFunction<String, Stat, ? extends T, Exception> factory) {
        return withFactoryEx((b, s) -> factory.apply(b == null ? null : new String(b), s));
    }

    /**
     * use {@link #withStringFactoryEx}
     */
    @Deprecated
    public GenericZkBasedNodeBuilder<T> withStringFactory(Function<String, ? extends T> factory) {
        return withStringFactoryEx(factory::apply);
    }

    public GenericZkBasedNodeBuilder<T>
            withStringFactoryEx(ThrowableFunction<String, ? extends T, Exception> factory) {
        return withStringFactoryEx((b, s) -> factory.apply(b));
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

    public GenericZkBasedNodeBuilder<T>
            withCleanupConsumer(ThrowableConsumer<? super T, Throwable> cleanup) {
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
