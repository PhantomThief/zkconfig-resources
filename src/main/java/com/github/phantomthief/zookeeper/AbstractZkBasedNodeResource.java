/**
 * 
 */
package com.github.phantomthief.zookeeper;

import java.util.function.Predicate;

import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;

/**
 * @author w.vela
 */
public abstract class AbstractZkBasedNodeResource<T> {

    protected final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(getClass());

    protected abstract T initObject(String zkValue);

    protected abstract NodeCache cache();

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

    protected T getResource() {
        if (resource == null) {
            synchronized (this) {
                if (resource == null) {
                    ChildData currentData = cache().getCurrentData();
                    if (currentData == null || currentData.getData() == null) {
                        return emptyObject();
                    }
                    resource = initObject(new String(currentData.getData()));
                    cache().getListenable().addListener(() -> {
                        T oldResource = null;
                        synchronized (this) {
                            ChildData data = cache().getCurrentData();
                            oldResource = resource;
                            if (data != null && data.getData() != null) {
                                resource = initObject(new String(data.getData()));
                            } else {
                                resource = emptyObject();
                            }
                        }
                        cleanup(oldResource);
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
}
