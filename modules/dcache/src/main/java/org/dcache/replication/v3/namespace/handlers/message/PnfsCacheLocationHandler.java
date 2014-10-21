package org.dcache.replication.v3.namespace.handlers.message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PnfsClearCacheLocationMessage;

import dmg.cells.nucleus.CellMessageReceiver;

import org.dcache.cells.CellStub;
import org.dcache.pool.migration.Task;
import org.dcache.pool.migration.TaskCompletionHandler;
import org.dcache.replication.v3.CellStubFactory;
import org.dcache.replication.v3.ReplicaManagerScheduledTaskExecutor;
import org.dcache.replication.v3.ReplicaManagerTaskExecutor;
import org.dcache.replication.v3.namespace.ResilientInfoCache;
import org.dcache.replication.v3.namespace.data.ResilientPoolGroupInfo;
import org.dcache.replication.v3.namespace.handlers.task.FileInfoTaskCompletionHandler;
import org.dcache.replication.v3.namespace.handlers.task.PoolGroupInfoTaskCompletionHandler;
import org.dcache.replication.v3.namespace.handlers.task.ReductionTaskCompletionHandler;
import org.dcache.replication.v3.namespace.tasks.FileInfoTask;
import org.dcache.replication.v3.namespace.tasks.PoolGroupInfoTask;
import org.dcache.replication.v3.namespace.tasks.ReductionTask;
import org.dcache.replication.v3.namespace.tasks.ReplicationTask;
import org.dcache.replication.v3.vehicles.CacheEntryInfoMessage;
import org.dcache.vehicles.PnfsSetFileAttributes;

/**
 * Receives {@link PnfsSetFileAttribute} messages and executes
 * {@link FileInfoTask} tasks.
 *
 * @author arossi
 */
public final class PnfsCacheLocationHandler
            implements CellMessageReceiver,
                       FileInfoTaskCompletionHandler,
                       PoolGroupInfoTaskCompletionHandler,
                       TaskCompletionHandler,
                       ReductionTaskCompletionHandler {
    private static final Logger LOGGER
        = LoggerFactory.getLogger(PnfsCacheLocationHandler.class);

    private ReplicaManagerTaskExecutor poolInfoTaskExecutor;
    private ReplicaManagerTaskExecutor pnfsInfoTaskExecutor;
    private ReplicaManagerScheduledTaskExecutor migrationTaskExecutor;
    private ReplicaManagerTaskExecutor reductionTaskExecutor;
    private ResilientInfoCache cache;

    private CellStub poolManager;
    private CellStub pnfsManager;
    private CellStub pinManager;
    private CellStubFactory poolStubFactory;

    private boolean useGreedyRequests;

    private MessageGuard guard;

    public void messageArrived(PnfsClearCacheLocationMessage message) {
        /*
         * Guard checks done on the message queue thread (there should be little
         * overhead).
         */
        if (!guard.acceptMessage("Clear Cache Location", message)) {
            return;
        }

        PnfsId pnfsId = message.getPnfsId();
        String pool = message.getPoolName();

        poolInfoTaskExecutor.execute(new PoolGroupInfoTask(pnfsId, pool, cache,
                        this));
        LOGGER.debug("executed ResilientPoolInfoTask for {}.", pool);
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
        poolInfoTaskExecutor.execute(new PoolGroupInfoTask(pnfsId, pool, cache,
                        this));
        LOGGER.debug("executed ResilientPoolInfoTask for {}.", pool);
    }

    public void setCache(ResilientInfoCache cache) {
        this.cache = cache;
    }

    public void setGuard(MessageGuard guard) {
        this.guard = guard;
    }

    public void setMigrationTaskExecutor(
                    ReplicaManagerScheduledTaskExecutor migrationTaskExecutor) {
        this.migrationTaskExecutor = migrationTaskExecutor;
    }

    public void setPinManager(CellStub pinManager) {
        this.pinManager = pinManager;
    }

    public void setPnfsInfoTaskExecutor(
                    ReplicaManagerTaskExecutor pnfsInfoTaskExecutor) {
        this.pnfsInfoTaskExecutor = pnfsInfoTaskExecutor;
    }

    public void setPnfsManager(CellStub pnfsManager) {
        this.pnfsManager = pnfsManager;
    }

    public void setPoolInfoTaskExecutor(
                    ReplicaManagerTaskExecutor poolInfoTaskExecutor) {
        this.poolInfoTaskExecutor = poolInfoTaskExecutor;
    }

    public void setPoolManager(CellStub poolManager) {
        this.poolManager = poolManager;
    }

    public void setPoolStubFactory(CellStubFactory poolStubFactory) {
        this.poolStubFactory = poolStubFactory;
    }

    public void setReductionTaskExecutor(
                    ReplicaManagerTaskExecutor reductionTaskExecutor) {
        this.reductionTaskExecutor = reductionTaskExecutor;
    }

    public void setUseGreedyRequests(boolean useGreedyRequests) {
        this.useGreedyRequests = useGreedyRequests;
    }

    @Override
    public void taskCancelled(CacheEntryInfoMessage info, String message) {
        if (info != null) {
            LOGGER.debug(message + ": {}@{}", info.pnfsId, info.getPool());
        } else {
            LOGGER.debug(message);
        }
    }

    @Override
    public void taskCancelled(ReductionTask task) {

    }

    @Override
    public void taskCancelled(Task task) {

    }

    public void taskCompleted(CacheEntryInfoMessage info) {
        /*
         * Check for permanent system sticky bit; if set, proceed with migration
         * request.
         */
        LOGGER.trace("PnfsSystemStickyInfoMessage messageArrived: {}.", info);
        if (!info.isSystemSticky()) {
            LOGGER.debug("{}@{}; was not system-sticky; discarding.",
                            info.pnfsId,
                            info.getPool());
            return;
        }

        /*
         * Issue the migration task with a completion handler. Should the task
         * complete with less confirmed locations than the current number, the
         * excess locations will be removed.
         */
        ReplicationTask task
            = new ReplicationTask(info,
                                  this,
                                  migrationTaskExecutor,
                                  cache,
                                  poolManager,
                                  pnfsManager,
                                  pinManager,
                                  useGreedyRequests);

        /*
         * The run method of this task can be executed on the pnfs executor
         * because it just accesses the cache in order to contruct the actual
         * migration task which uses its own executor.
         */
        pnfsInfoTaskExecutor.execute(task);
    }

    @Override
    public void taskCompleted(PnfsId pnfsId, String pool, ResilientPoolGroupInfo info) {
        if (!info.isResilient()) {
            LOGGER.debug("{} does not belong to a resilient group; "
                            + "ignoring message for {}.",
                            pool, pnfsId);
            return;
        }

        /*
         * The next step is to find out specifically whether the file in this
         * location is CACHED+system. The task first gets the file attributes
         * (potentially a database call), then sends an asynchronous message to
         * the pool.
         */
        FileInfoTask task
            = new FileInfoTask(pnfsId,
                               poolStubFactory.getCellStub(pool),
                               this,
                               cache,
                               pnfsInfoTaskExecutor);
        pnfsInfoTaskExecutor.execute(task);
        LOGGER.debug("executed FileInfoTask for {}.", pnfsId);
    }

    @Override
    public void taskCompleted(ReductionTask task) {

    }

    @Override
    public void taskCompleted(Task task) {
        /*
         * Post process the task for excess copies.
         */
        reductionTaskExecutor.execute(new ReductionTask(task.getPnfsId(),
                                                        null, // task.getConfirmedLocations(),
                                                        cache,
                                                        this));
    }

    @Override
    public void taskFailed(CacheEntryInfoMessage info, Exception exception) {

    }

    public void taskFailed(PnfsId pnfsId, String pool, Exception exception) {
        // TODO Auto-generated method stub

    }

    @Override
    public void taskFailed(ReductionTask task) {

    }

    @Override
    public void taskFailed(Task task, String msg) {

    }

    @Override
    public void taskFailedPermanently(ReductionTask task) {

    }

    @Override
    public void taskFailedPermanently(Task task, String msg) {

    }
}
