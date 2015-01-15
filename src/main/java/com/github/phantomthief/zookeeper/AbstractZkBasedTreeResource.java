/**
 * 
 */
package com.github.phantomthief.zookeeper;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.Predicate;

import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type;
import org.apache.curator.utils.ZKPaths;

/**
 * @author w.vela
 */
public abstract class AbstractZkBasedTreeResource<T> implements Closeable {

    protected final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(getClass());

    protected abstract T initObject(String zkValue);

    protected abstract String monitorPath();

    protected abstract PathChildrenCache cache();

    protected Predicate<T> doCleanupOperation() {
        return null;
    }

    protected long waitStopPeriod() {
        return 0;
    }

    protected T emptyObject() {
        return null;
    }

    protected volatile T resource;

    private final Object lock = new Object();

    protected T getResource() {
        if (resource == null) {
            synchronized (lock) {
                if (resource == null) {
                    ChildData currentData = cache().getCurrentData(monitorPath());
                    if (currentData == null || currentData.getData() == null) {
                        return emptyObject();
                    }
                    resource = initObject(new String(currentData.getData()));
                    cache().getListenable().addListener(
                            (client, event) -> {
                                if (event.getType() == Type.CHILD_UPDATED
                                        && ZKPaths.getNodeFromPath(event.getData().getPath())
                                                .equals(monitorPath())) {

                                    T oldResource = null;
                                    synchronized (lock) {
                                        ChildData data = cache().getCurrentData(monitorPath());
                                        oldResource = resource;
                                        if (data != null && data.getData() != null) {
                                            resource = initObject(new String(data.getData()));
                                        } else {
                                            resource = emptyObject();
                                        }
                                    }
                                    cleanup(oldResource);
                                }
                            });
                }
            }
        }
        return resource;
    }

    /**
     * @param oldResource
     */
    protected void cleanup(T oldResource) {
        if (oldResource != null) {
            Predicate<T> doCleanupOperation = doCleanupOperation();
            if (doCleanupOperation == null) {
                return;
            }
            Thread cleanupThread = new Thread("old [" + oldResource.getClass().getSimpleName()
                    + "] cleanup thread-[" + oldResource.hashCode() + "]") {

                @Override
                public void run() {
                    do {
                        long waitStopPeriod = waitStopPeriod();
                        if (waitStopPeriod > 0) {
                            try {
                                Thread.sleep(waitStopPeriod);
                            } catch (InterruptedException e) {
                                logger.error("Ops.", e);
                            }
                        }
                        if (doCleanupOperation.test(oldResource)) {
                            break;
                        }
                    } while (true);
                    logger.info("successfully close old resource:{}", oldResource);
                }
            };
            cleanupThread.start();
        }
    }

    @Override
    public void close() throws IOException {
        synchronized (lock) {
            Predicate<T> cleanOp;
            if (resource != null && (cleanOp = doCleanupOperation()) != null) {
                cleanOp.test(resource);
            }
        }
    }

}
