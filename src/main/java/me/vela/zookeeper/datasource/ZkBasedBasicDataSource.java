/**
 * 
 */
package me.vela.zookeeper.datasource;

import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import me.vela.util.ObjectMapperUtils;
import me.vela.zookeeper.AbstractZkBasedResource;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;

/**
 * @author w.vela
 */
public class ZkBasedBasicDataSource extends AbstractZkBasedResource<BasicDataSource> {

    private final String monitorPath;

    private final PathChildrenCache cache;

    /**
     * @param monitorPath
     * @param cache
     */
    public ZkBasedBasicDataSource(String monitorPath, PathChildrenCache cache) {
        this.monitorPath = monitorPath;
        this.cache = cache;
    }

    /* (non-Javadoc)
     * @see me.vela.zookeeper.AbstractZkBasedResource#initObject(java.lang.String)
     */
    @Override
    protected BasicDataSource initObject(String rawNode) {
        try {
            Map<String, Object> node = ObjectMapperUtils.fromJSON(rawNode, Map.class, String.class,
                    Object.class);

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

            BeanUtils.populate(dataSource, node);

            logger.info("build datasource for {}, {}", monitorPath, url);
            return dataSource;
        } catch (Throwable e) {
            logger.error("Ops, fail to build dataSource:{}.", monitorPath, e);
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

    /* (non-Javadoc)
     * @see me.vela.zookeeper.AbstractZkBasedResource#doCleanup(java.lang.Object)
     */
    @Override
    protected boolean doCleanup(BasicDataSource oldResource) {
        if (oldResource.isClosed()) {
            return true;
        }
        try {
            oldResource.close();
            return oldResource.isClosed();
        } catch (SQLException e) {
            logger.error("fail to close old dataSource:{}", oldResource, e);
            return false;
        }
    }

}
