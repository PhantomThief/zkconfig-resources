zkconfig-resources
=======================

A ZooKeeper config based MySQL and jedis connection pool wrapper.

* Pool config changes on the fly
* MySQL connection pool support sharding
* jdk 1.8 only

## Get Started

```xml
<dependency>
    <groupId>com.github.phantomthief</groupId>
    <artifactId>zkconfig-resources</artifactId>
    <version>1.0.0</version>
</dependency>
```

## Usage

Redis connection pool example:

```Java
try (CuratorFramework client = CuratorFrameworkFactory.newClient("127.0.0.1:2181",
        new ExponentialBackoffRetry(10000, 3))) {
    client.start(); // init a curator

    ZkBasedJedis jedis = new ZkBasedJedis("/jedis/clientConfig", client); // declare a jedis client using a config from zk's node. 

    jedis.get().set("key1", "value1"); // exec jedis commands, don't worry about returning the connection back to the pool.
    System.out.println("get key:" + jedis.get().get("key1")); // exec jedis commands.

    // pipeline operation
    Random random = new Random();
    List<Integer> list = Arrays.asList(1, 2, 3, 4);

    jedis.pipeline(list,
            (pipeline, key) -> pipeline.sadd("prefix:" + key, random.nextInt(100) + "")); // pipeline write

    Map<Integer, Long> multiCount = jedis.pipeline(list,
            (pipeline, key) -> pipeline.scard("prefix:" + key)); // pipeline read
    System.out.println(multiCount);

    Map<Integer, Boolean> customCodecPipelineResult = jedis.pipeline(list,
            (pipeline, key) -> pipeline.scard("prefix:" + key), value -> value > 10); // pipeline with customize decoder
    System.out.println(customCodecPipelineResult);
}
```

MySQL connection pool example:

```Java
try (CuratorFramework client = CuratorFrameworkFactory.newClient("127.0.0.1:2181",
        new ExponentialBackoffRetry(10000, 3))) {
    client.start(); // init a curator

    DataSource dataSource = new ZkBasedBasicDataSource("/dataSource/defaultMySqlConfig",
            client);
    // using the dataSource as you like.
}
```

## Configuration

Redis connection pool config example:

```
create /jedis/clientConfig {"t1":"127.0.0.1:6379","t2":"127.0.0.1:6380","t3@p@ssword":"127.0.0.1:6381"}
```

Data source config example:

```
create /dataSource/defaultMySqlConfig {"pass":"p@ssword","user":"root","url":"jdbc:mysql://127.0.0.1:3306/test"}
```