package org.dcache.replication.v3.namespace.handlers.message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PnfsClearCacheLocationMessage;

import dmg.cells.nucleus.CellMessageReceiver;

import org.dcache.replication.v3.CDCFixedPoolTaskExecutor;
import org.dcache.replication.v3.namespace.ResilientInfoCache;
import org.dcache.replication.v3.namespace.handlers.task.PoolGroupInfoTaskCompletionHandler;
import org.dcache.replication.v3.namespace.tasks.PoolGroupInfoTask;
import org.dcache.vehicles.PnfsSetFileAttributes;

/**
 * @author arossi
 */
public final class PnfsCacheLocationHandler implements CellMessageReceiver {
    private static final Logger LOGGER
        = LoggerFactory.getLogger(PnfsCacheLocationHandler.class);

    private CDCFixedPoolTaskExecutor executor;
    private ResilientInfoCache cache;
    private MessageGuard guard;
    private PoolGroupInfoTaskCompletionHandler completionHandler;

    public void messageArrived(PnfsClearCacheLocationMessage message) {
        /*
         * Guard checks done on the message queue thread
         * (there should be little overhead).
         */
        if (!guard.acceptMessage("Clear Cache Location", message)) {
            return;
        }

        PnfsId pnfsId = message.getPnfsId();
        String pool = message.getPoolName();

        executor.execute(new PoolGroupInfoTask(pnfsId,
                                               pool,
                                               cache,
                                               completionHandler));
        LOGGER.debug("executed PoolGroupInfoTask for {}.", pool);
    }

    public void messageArrived(PnfsSetFileAttributes message) {
        /*
         * Guard checks done on the message queue thread (there should be little
         * overhead).
         */
        if (!guard.acceptMessage("Set File Attributes", message)) {
            return;
        }

        PnfsId pnfsId = message.getPnfsId();
        Collection<String> locations = message.getFileAttributes().getLocations();

        /*
         * We are only interested in attribute updates where a single new
         * location is added.
         */
        if (locations.size() != 1) {
            LOGGER.debug("Message for {} contains {} locations ({}): "
                            + "irrelevant to replication; " + "discarding.",
                            pnfsId, locations.size(), locations);
            return;
        }

        /*
         * Offload request for resilient pool information onto separate thread.
         * Results processed by PoolGroupInfoHandler#handleDone.
         */
        String pool = locations.iterator().next();
        executor.execute(new PoolGroupInfoTask(pnfsId,
                                               pool,
                                               cache,
                                               completionHandler));
        LOGGER.debug("executed PoolGroupInfoTask for {}.", pool);
    }

    public void setCache(ResilientInfoCache cache) {
        this.cache = cache;
    }

    public void setGuard(MessageGuard guard) {
        this.guard = guard;
    }

    public void setPoolInfoTaskExecutor(
                    CDCFixedPoolTaskExecutor poolInfoTaskExecutor) {
        this.executor = poolInfoTaskExecutor;
    }

    public PoolGroupInfoTaskCompletionHandler getPoolGroupInfoTaskHandler() {
        return completionHandler;
    }

    public void setPoolGroupInfoTaskHandler(PoolGroupInfoTaskCompletionHandler
                                            completionHandler) {
        this.completionHandler = completionHandler;
    }
}
