/**
 * 
 */
package me.vela.zookeeper.datasource;

import java.sql.SQLException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.sql.DataSource;

import me.vela.util.ObjectMapperUtils;
import me.vela.util.WeakHolder;
import me.vela.zookeeper.AbstractZkBasedResource;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.ImmutableRangeMap.Builder;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;

/**
 * @author w.vela
 */
public class ZkBasedShardedBasicDataSource extends
        AbstractZkBasedResource<RangeMap<Integer, BasicDataSource>> {

    private static WeakHolder<Pattern> shardPattern = WeakHolder.of(() -> Pattern
            .compile("(\\d+)-(\\d+)")); //(\d+)-(\d+)

    private final String monitorPath;

    private final PathChildrenCache cache;

    {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            logger.error("Ops.", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * @param monitorPath
     * @param cache
     */
    public ZkBasedShardedBasicDataSource(String monitorPath, PathChildrenCache cache) {
        this.monitorPath = monitorPath;
        this.cache = cache;
    }

    /* (non-Javadoc)
     * @see me.vela.zookeeper.AbstractZkBasedResource#initObject(java.lang.String)
     */
    @SuppressWarnings("unchecked")
    @Override
    protected RangeMap<Integer, BasicDataSource> initObject(String rawNode) {
        try {
            /**
             * 原记录格式{singleDataSourceConfig}
             * 散表记录格式{"0-5":{singleDataSourceConfig1},"6-10":{
             * singleDataSourceConfig2}}
             */
            Map<String, Object> allShard = ObjectMapperUtils.fromJSON(rawNode, Map.class,
                    String.class, Object.class);
            Builder<Integer, BasicDataSource> resultBuilder = ImmutableRangeMap
                    .<Integer, BasicDataSource> builder();

            for (Entry<String, Object> entry : allShard.entrySet()) {
                Map<String, Object> node = (Map<String, Object>) entry.getValue();

                String url = (String) node.get("url");
                String user = (String) node.get("user");
                String pass = (String) node.get("pass");

                BasicDataSource dataSource = new BasicDataSource();
                dataSource.setUrl(url);
                dataSource.setUsername(user);
                dataSource.setPassword(pass);
                dataSource.setMinIdle(1);
                dataSource.setMaxIdle(10);
                dataSource.setMaxTotal(-1);
                dataSource.setDefaultAutoCommit(true);
                dataSource.setMinEvictableIdleTimeMillis(TimeUnit.MINUTES.toMillis(1));
                dataSource.setSoftMinEvictableIdleTimeMillis(TimeUnit.MINUTES.toMillis(1));
                dataSource.setTestOnBorrow(true);
                dataSource.setTestWhileIdle(true);
                dataSource.setValidationQuery("/* ping */");
                logger.info("build datasource for {}, {}", monitorPath, url);

                BeanUtils.populate(dataSource, node);

                // 解析出shard范围
                Matcher matcher = shardPattern.get().matcher(entry.getKey());
                if (matcher.find()) {
                    int start = Integer.parseInt(matcher.group(1));
                    int end = Integer.parseInt(matcher.group(2));
                    resultBuilder.put(Range.closed(start, end), dataSource);
                } else {
                    logger.error("invalid shard config:{}", rawNode);
                }

            }

            ImmutableRangeMap<Integer, BasicDataSource> result = resultBuilder.build();
            // checking valid
            int shardSize = getShardSize(result);
            for (int i = 0; i < shardSize; i++) {
                if (result.get(i) == null) {
                    logger.error("error shard config on validate:{}, fail on shard:{}", rawNode, i);
                }
            }
            return result;
        } catch (Throwable e) {
            logger.error("Ops. fail to init shard dataSource:{}", monitorPath, e);
            throw new RuntimeException(e);
        }
    }

    /* (non-Javadoc)
     * @see me.vela.zookeeper.AbstractZkBasedResource#monitorPath()
     */
    @Override
    protected String monitorPath() {
        return monitorPath;
    }

    /* (non-Javadoc)
     * @see me.vela.zookeeper.AbstractZkBasedResource#cache()
     */
    @Override
    protected PathChildrenCache cache() {
        return cache;
    }

    @Override
    protected Predicate<RangeMap<Integer, BasicDataSource>> doCleanupOperation() {
        return oldResource -> {
            boolean allSuccess = true;
            for (BasicDataSource dataSource : oldResource.asMapOfRanges().values()) {
                if (dataSource.isClosed()) {
                    continue;
                }
                try {
                    dataSource.close();
                    if (!dataSource.isClosed()) {
                        allSuccess = false;
                    }
                } catch (SQLException e) {
                    logger.error("fail to close datasource:{}", dataSource, e);
                }
            }
            return allSuccess;
        };
    }

    public BasicDataSource getDataSource(int shard) {
        RangeMap<Integer, BasicDataSource> thisObject = getResource();
        return thisObject.get(shard % getShardSize(thisObject));
    }

    public BasicDataSource getDataSource(long shard) {
        RangeMap<Integer, BasicDataSource> thisObject = getResource();
        return thisObject.get((int) (shard % getShardSize(thisObject)));
    }

    public String processTableName(String rawTableName, int shard) {
        RangeMap<Integer, BasicDataSource> thisObject = getResource();
        return rawTableName + "_" + shard % getShardSize(thisObject);
    }

    public String processTableName(String rawTableName, long shard) {
        RangeMap<Integer, BasicDataSource> thisObject = getResource();
        return rawTableName + "_" + shard % getShardSize(thisObject);
    }

    public int getShard(int shard) {
        RangeMap<Integer, BasicDataSource> thisObject = getResource();
        return shard % getShardSize(thisObject);
    }

    public int getShard(long shard) {
        RangeMap<Integer, BasicDataSource> thisObject = getResource();
        return (int) (shard % getShardSize(thisObject));
    }

    private static final <T> int getShardSize(RangeMap<Integer, T> rangeMap) {
        return rangeMap.span().upperEndpoint() + 1;
    }

    private static ConcurrentMap<DataSource, NamedParameterJdbcTemplate> jdbcTemplateCache = new MapMaker()
            .concurrencyLevel(16).weakKeys().weakValues().makeMap();

    public static final NamedParameterJdbcTemplate of(DataSource ds) {
        return jdbcTemplateCache.computeIfAbsent(ds, d -> {
            NamedParameterJdbcTemplate r = new NamedParameterJdbcTemplate(d);
            r.setCacheLimit(0);
            return r;
        });
    }

}
