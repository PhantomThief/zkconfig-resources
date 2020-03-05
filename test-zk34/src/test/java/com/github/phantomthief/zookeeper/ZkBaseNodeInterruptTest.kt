package com.github.phantomthief.zookeeper

import com.github.phantomthief.util.ThrowableFunction
import com.github.phantomthief.zookeeper.util.ZkUtils.setToZk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

/**
 * @author w.vela
 * Created on 2018-09-08.
 */
class ZkBaseNodeInterruptTest : BaseTest() {

    private lateinit var zkNode: ZkBasedNodeResource<String>

    @BeforeEach
    internal fun setUp() {
        val path = "/testInt"
        setToZk(curatorFramework, path, "value".toByteArray())
        zkNode = ZkBasedNodeResource.newGenericBuilder<String>()
            .withCacheFactory(path, curatorFramework)
            .withFactoryEx(ThrowableFunction { String(it) })
            .build()
    }

    @Test
    internal fun test() {
        Thread.currentThread().interrupt()
        assertEquals("value", zkNode.get())
    }
}