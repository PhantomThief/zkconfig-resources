/**
 * 
 */
package me.vela.zookeeper.datasource;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import javax.sql.DataSource;

import me.vela.util.ObjectMapperUtils;
import me.vela.zookeeper.AbstractZkBasedResource;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;

/**
 * @author w.vela
 */
public class ZkBasedBasicDataSource extends AbstractZkBasedResource<BasicDataSource> implements
        DataSource {

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

    /* (non-Javadoc)
     * @see javax.sql.CommonDataSource#getLogWriter()
     */
    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return getResource().getLogWriter();
    }

    /* (non-Javadoc)
     * @see javax.sql.CommonDataSource#setLogWriter(java.io.PrintWriter)
     */
    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        getResource().setLogWriter(out);
    }

    /* (non-Javadoc)
     * @see javax.sql.CommonDataSource#setLoginTimeout(int)
     */
    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        getResource().setLoginTimeout(seconds);
    }

    /* (non-Javadoc)
     * @see javax.sql.CommonDataSource#getLoginTimeout()
     */
    @Override
    public int getLoginTimeout() throws SQLException {
        return getResource().getLoginTimeout();
    }

    /* (non-Javadoc)
     * @see javax.sql.CommonDataSource#getParentLogger()
     */
    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return getResource().getParentLogger();
    }

    /* (non-Javadoc)
     * @see java.sql.Wrapper#unwrap(java.lang.Class)
     */
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return (T) getResource().unwrap(iface);
    }

    /* (non-Javadoc)
     * @see java.sql.Wrapper#isWrapperFor(java.lang.Class)
     */
    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return getResource().isWrapperFor(iface);
    }

    /* (non-Javadoc)
     * @see javax.sql.DataSource#getConnection()
     */
    @Override
    public Connection getConnection() throws SQLException {
        return getResource().getConnection();
    }

    /* (non-Javadoc)
     * @see javax.sql.DataSource#getConnection(java.lang.String, java.lang.String)
     */
    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return getResource().getConnection(username, password);
    }

}
