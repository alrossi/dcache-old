package org.dcache.replication.v3.namespace.handlers.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

import org.dcache.cells.CellStub;
import org.dcache.replication.v3.CDCFixedPoolTaskExecutor;
import org.dcache.replication.v3.CDCScheduledPoolTaskExecutor;
import org.dcache.replication.v3.namespace.ResilientInfoCache;
import org.dcache.replication.v3.namespace.tasks.ReplicationTask;
import org.dcache.replication.v3.vehicles.CacheEntryInfoMessage;

/**
 * @author arossi
 *
 */
public class FileInfoTaskCompletionHandler {
    private static final Logger LOGGER
        = LoggerFactory.getLogger(FileInfoTaskCompletionHandler.class);

    private ResilientInfoCache cache;
    private CDCScheduledPoolTaskExecutor migrationTaskExecutor;
    private CDCFixedPoolTaskExecutor pnfsInfoTaskExecutor;

    private CellStub poolManager;
    private CellStub pnfsManager;
    private CellStub pinManager;

    private boolean useGreedyRequests;

    public void taskCompleted(CacheEntryInfoMessage info, Set<String> tried) {
        LOGGER.trace("CacheEntryInfoMessage messageArrived: {}.", info);

        if (tried == null) {
            tried = new HashSet<>();
        }

        tried.add(info.getPool());

        /*
         * Issue the migration task with a completion handler. Should the task
         * complete with less confirmed locations than the current number, the
         * excess locations will be removed.
         */
        ReplicationTask task = new ReplicationTask(info,
                                                   new ReplicationTaskCompletionHandler(tried),
                                                   migrationTaskExecutor,
                                                   cache,
                                                   poolManager,
                                                   pnfsManager,
                                                   pinManager,
                                                   useGreedyRequests);

        /*
         * The run method of this task can be executed on the pnfs executor
         * because it just accesses the cache in order to contruct the actual
         * migration task which then uses its own executor.
         */
        pnfsInfoTaskExecutor.execute(task);
    }

    public void taskFailed(CacheEntryInfoMessage info, String message) {

    }

    public void taskCancelled(CacheEntryInfoMessage info, String message) {
        if (info != null) {
            LOGGER.debug(message + ": {}@{}", info.pnfsId, info.getPool());
        } else {
            /*
             * The cancellation can occur if the access latency for the
             * file was for some reason not ONLINE. In this case, the
             * info object is <code>null</code>.
             */
            LOGGER.debug(message);
        }
    }
}
