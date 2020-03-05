package com.github.phantomthief.zookeeper

import com.github.phantomthief.zookeeper.util.ZkUtils.setToZk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

/**
 * @author w.vela
 * Created on 2018-09-08.
 */
class ZkBaseTreeInterruptTest : BaseTest() {

    private lateinit var zkTree: ZkBasedTreeNodeResource<String>

    @BeforeEach
    internal fun setUp() {
        val path = "/testInt2"
        setToZk(curatorFramework, path, "value".toByteArray())
        setToZk(curatorFramework, "$path/test", "value".toByteArray())
        zkTree = ZkBasedTreeNodeResource.newBuilder<String>()
            .curator(curatorFramework)
            .path(path)
            .childDataFactoryEx { col -> col.joinToString { String(it.data) } }
            .build()
    }

    @Test
    internal fun test() {
        Thread.currentThread().interrupt()
        assertEquals("value", zkTree.get())
    }
}