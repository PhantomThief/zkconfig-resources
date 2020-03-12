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

## 使用test-zk34测试兼容性

test-zk34默认会打一个bin二进制包，可以利用二进制包来进行curator，zookeeper-client，zookeeper-server各种版本组合进行兼容性的测试。

```Shell
# 指定curator和zookeeper-client的版本
mvn -Dcurator.version=4.2.0 -Dzookeeper.version=3.6.0 clean install -DskipTests
cd test-zk34/target
tar -xzvf zkconfig-resources-test-zk34-${version}-bin.tar.gz
cd zkconfig-resources-test-zk34-${version}-bin

# 连接localhost:2181并运行所有测试，并强制curator使用zk34兼容模式
TEST_PROP="-Dzk.zk34CompatibilityMode=true" sh bin/run_tests.sh "localhost:2181"

# 没有指定脚本参数，会使用curator-test内置的TestingServer进行测试
sh bin/run_tests.sh
```
