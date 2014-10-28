package org.dcache.replication.v3.namespace.handlers.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import diskCacheV111.util.PnfsId;

import org.dcache.replication.v3.CDCFixedPoolTaskExecutor;
import org.dcache.replication.v3.CellStubFactory;
import org.dcache.replication.v3.namespace.ResilientInfoCache;
import org.dcache.replication.v3.namespace.data.PoolGroupInfo;
import org.dcache.replication.v3.namespace.tasks.FileInfoTask;

/**
 * @author arossi
 */
public class PoolGroupInfoTaskCompletionHandler {
    private static final Logger LOGGER
        = LoggerFactory.getLogger(PoolGroupInfoTaskCompletionHandler.class);

    private CellStubFactory poolStubFactory;
    private CDCFixedPoolTaskExecutor pnfsInfoTaskExecutor;
    private ResilientInfoCache cache;
    private FileInfoTaskCompletionHandler fileInfoTaskHandler;

    public void taskCompleted(PnfsId pnfsId, String pool, PoolGroupInfo info, Set<String> triedPools) {
        if (!info.isResilient()) {
            LOGGER.debug("{} does not belong to a resilient group; "
                            + "discarding message for {}.", pool, pnfsId);
            return;
        }

        /*
         * The next step is to ensure the file in this location is actually
         * CACHED+system. The task first gets the file attributes (potentially a
         * database call), then sends an asynchronous message to the pool,
         * enforcing the sticky bit and returning the cache entry object to pass
         * on to the migration task.
         */
        FileInfoTask task = new FileInfoTask(pnfsId,
                                             poolStubFactory.getCellStub(pool),
                                             fileInfoTaskHandler,
                                             cache,
                                             pnfsInfoTaskExecutor,
                                             triedPools);
        pnfsInfoTaskExecutor.execute(task);
        LOGGER.debug("executed FileInfoTask for {}.", pnfsId);
    }

    public void taskFailed(PnfsId pnfsId, String pool, String message) {

    }
}
