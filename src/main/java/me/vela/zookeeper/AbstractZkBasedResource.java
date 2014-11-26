/**
 * 
 */
package me.vela.zookeeper;

import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type;
import org.apache.curator.utils.ZKPaths;

/**
 * @author w.vela
 */
public abstract class AbstractZkBasedResource<T> {

    protected final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(getClass());

    protected abstract T initObject(String zkValue);

    protected abstract String monitorPath();

    protected abstract PathChildrenCache cache();

    protected abstract boolean doCleanup(T oldResource);

    protected long waitStopPeriod() {
        return TimeUnit.MINUTES.toMillis(1);
    }

    protected volatile T resource;

    protected T getResource() {
        if (resource == null) {
            synchronized (this) {
                if (resource == null) {
                    ChildData currentData = cache().getCurrentData(monitorPath());
                    if (currentData == null || currentData.getData() == null) {
                        return null;
                    }
                    resource = initObject(new String(currentData.getData()));
                    cache().getListenable().addListener(
                            (client, event) -> {
                                if (event.getType() == Type.CHILD_UPDATED
                                        && ZKPaths.getNodeFromPath(event.getData().getPath())
                                                .equals(monitorPath())) {

                                    T oldResource = null;
                                    synchronized (this) {
                                        ChildData data = cache().getCurrentData(monitorPath());
                                        oldResource = resource;
                                        if (data != null && data.getData() != null) {
                                            resource = initObject(new String(data.getData()));
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
    private void cleanup(T oldResource) {
        if (oldResource != null) {
            Thread cleanupThread = new Thread("old " + oldResource.getClass().getSimpleName()
                    + " cleanup thread-" + oldResource.hashCode()) {

                @Override
                public void run() {
                    do {
                        try {
                            Thread.sleep(waitStopPeriod());
                        } catch (InterruptedException e) {
                            logger.error("Ops.", e);
                        }
                        if (doCleanup(oldResource)) {
                            break;
                        }
                    } while (true);
                }
            };
            cleanupThread.start();
        }
    }
}
