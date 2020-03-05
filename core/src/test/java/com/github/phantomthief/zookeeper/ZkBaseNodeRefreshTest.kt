package com.github.phantomthief.zookeeper

import com.github.phantomthief.util.ThrowableFunction
import com.github.phantomthief.zookeeper.util.ZkUtils.setToZk
import com.google.common.util.concurrent.Futures.immediateFuture
import com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService
import com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.concurrent.TimeUnit.SECONDS

/**
 * @author w.vela
 * Created on 2018-07-18.
 */
internal object ZkBaseNodeRefreshTest : BaseTest() {

    private const val path: String = "/testRefresh"

    @Test
    internal fun test() {
        setToZk(curatorFramework, path, "value1".toByteArray())
        val nodes: Set<ZkBasedNodeResource<String>> = buildNodes()
        nodes.forEach { assertEquals(it.get(), "value1") }
        setToZk(curatorFramework, path, "value2".toByteArray())
        sleepUninterruptibly(3, SECONDS)
        nodes.forEach { assertEquals(it.get(), "value2!") }
        setToZk(curatorFramework, path, "value3".toByteArray())
        sleepUninterruptibly(3, SECONDS)
        nodes.forEach { assertEquals(it.get(), "value3!") }
    }

    private fun buildNodes(): Set<ZkBasedNodeResource<String>> {
        val set = HashSet<ZkBasedNodeResource<String>>()
        set.add(genericVer { it.withAsyncRefreshFactory { bs, _ -> immediateFuture("${String(bs)}!") } })
        set.add(genericVer { it.withAsyncRefreshFactory { bs -> immediateFuture("${String(bs)}!") } })
        set.add(genericVer { it.withRefreshFactory { bs, _ -> "${String(bs)}!" } })
        set.add(genericVer { it.withRefreshFactory { bs -> "${String(bs)}!" } })
        set.add(genericVer { it.withRefreshFactory(newDirectExecutorService()) { bs, _ -> "${String(bs)}!" } })
        set.add(genericVer { it.withRefreshFactory(newDirectExecutorService()) { bs -> "${String(bs)}!" } })

        set.add(genericVer { it.withAsyncRefreshStringFactory { bs, _ -> immediateFuture("$bs!") } })
        set.add(genericVer { it.withAsyncRefreshStringFactory { bs -> immediateFuture("$bs!") } })
        set.add(genericVer { it.withRefreshStringFactory { bs, _ -> "$bs!" } })
        set.add(genericVer { it.withRefreshStringFactory { bs -> "$bs!" } })
        set.add(genericVer { it.withRefreshStringFactory(newDirectExecutorService()) { bs, _ -> "$bs!" } })
        set.add(genericVer { it.withRefreshStringFactory(newDirectExecutorService()) { bs -> "$bs!" } })

        set.add(rawVer { it.withAsyncRefreshFactory { bs, _ -> immediateFuture("${String(bs)}!") } })
        set.add(rawVer { it.withAsyncRefreshFactory { bs -> immediateFuture("${String(bs)}!") } })
        set.add(rawVer { it.withRefreshFactory { bs, _ -> "${String(bs)}!" } })
        set.add(rawVer { it.withRefreshFactory { bs -> "${String(bs)}!" } })
        set.add(rawVer { it.withRefreshFactory(newDirectExecutorService()) { bs, _ -> "${String(bs)}!" } })
        set.add(rawVer { it.withRefreshFactory(newDirectExecutorService()) { bs -> "${String(bs)}!" } })

        set.add(rawVer { it.withAsyncRefreshStringFactory { bs, _ -> immediateFuture("$bs!") } })
        set.add(rawVer { it.withAsyncRefreshStringFactory { bs -> immediateFuture("$bs!") } })
        set.add(rawVer { it.withRefreshStringFactory { bs, _ -> "$bs!" } })
        set.add(rawVer { it.withRefreshStringFactory { bs -> "$bs!" } })
        set.add(rawVer { it.withRefreshStringFactory(newDirectExecutorService()) { bs, _ -> "$bs!" } })
        set.add(rawVer { it.withRefreshStringFactory(newDirectExecutorService()) { bs -> "$bs!" } })
        return set
    }

    private fun genericVer(func: (GenericZkBasedNodeBuilder<String>) -> GenericZkBasedNodeBuilder<String>): ZkBasedNodeResource<String> {
        var builder = ZkBasedNodeResource.newGenericBuilder<String>()
            .withCacheFactory(path, curatorFramework)
            .withStringFactoryEx(ThrowableFunction { it })
        builder = func.invoke(builder)
        return builder.build()
    }

    private fun rawVer(func: (ZkBasedNodeResource.Builder<String>) -> ZkBasedNodeResource.Builder<String>): ZkBasedNodeResource<String> {
        var builder = ZkBasedNodeResource.newBuilder()
            .withCacheFactory(path, curatorFramework)
            .withStringFactoryEx<String>(ThrowableFunction { it })
        builder = func.invoke(builder)
        return builder.build()
    }
}