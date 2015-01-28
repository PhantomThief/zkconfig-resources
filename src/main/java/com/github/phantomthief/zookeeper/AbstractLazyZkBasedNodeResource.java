/**
 * 
 */
package com.github.phantomthief.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.NodeCache;

import com.google.common.base.Supplier;

/**
 * @author w.vela
 */
public abstract class AbstractLazyZkBasedNodeResource<T> extends AbstractZkBasedNodeResource<T> {

    protected final String monitorPath;

    private final CuratorFramework client;

    private final Supplier<CuratorFramework> clientFactory;

    private volatile NodeCache cache;

    /**
     * @param monitorPath
     * @param client
     * @param clientFactory
     */
    private AbstractLazyZkBasedNodeResource(String monitorPath, CuratorFramework client,
            Supplier<CuratorFramework> clientFactory) {
        this.monitorPath = monitorPath;
        this.client = client;
        this.clientFactory = clientFactory;
    }

    /**
     * @param monitorPath
     * @param client
     */
    public AbstractLazyZkBasedNodeResource(String monitorPath, CuratorFramework client) {
        this(monitorPath, client, null);
    }

    /**
     * @param monitorPath
     * @param clientFactory
     */
    public AbstractLazyZkBasedNodeResource(String monitorPath,
            Supplier<CuratorFramework> clientFactory) {
        this(monitorPath, null, clientFactory);
    }

    protected NodeCache cache() {
        if (cache == null) {
            synchronized (this) {
                if (cache == null) {
                    CuratorFramework thisClient = null;
                    if (client != null) {
                        if (client.getState() != CuratorFrameworkState.STARTED) {
                            client.start();
                            thisClient = client;
                        }
                    }
                    if (clientFactory != null) {
                        thisClient = clientFactory.get();
                    }
                    if (thisClient == null) {
                        throw new RuntimeException(
                                "there is no curator framework or client factory found.");
                    }
                    NodeCache buildingCache = new NodeCache(thisClient, monitorPath);
                    try {
                        buildingCache.start();
                        buildingCache.rebuild();
                        this.cache = buildingCache;
                    } catch (Throwable e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        return cache;
    }

}
