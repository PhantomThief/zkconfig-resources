zkconfig-resources
=======================
[![Build Status](https://travis-ci.org/PhantomThief/zkconfig-resources.svg)](https://travis-ci.org/PhantomThief/zkconfig-resources)
[![Coverage Status](https://coveralls.io/repos/github/PhantomThief/zkconfig-resources/badge.svg?branch=master)](https://coveralls.io/github/PhantomThief/zkconfig-resources?branch=master)
[![Maven Central](https://img.shields.io/maven-central/v/com.github.phantomthief/zkconfig-resources)](https://search.maven.org/artifact/com.github.phantomthief/zkconfig-resources/)

ZooKeeper节点缓存，基于CuratorFramework封装

* 节点变更时，旧资源清理机制
* 强类型泛型支持
* 只支持Java8

## Get Started

```Java
ZkBasedNodeResource<ShardedJedisPool> node = ZkBasedNodeResource.<ShardedJedisPool> newBuilder()
                .withCacheFactory(ZKPaths.makePath("/redis", monitorPath), ZkClientHolder::get)
                .withStringFactory(this::initObject)
                .withCleanup(ShardedJedisPool::close)
                .build();

ShardedJedisPool pool = node.get();                
```
