package org.dcache.replication.v3.namespace.handlers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import diskCacheV111.namespace.NameSpaceProvider;
import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.util.PnfsId;

import dmg.cells.nucleus.CDC;
import dmg.cells.nucleus.CellMessageReceiver;

import org.dcache.cells.CellStub;
import org.dcache.pool.migration.Task;
import org.dcache.pool.migration.TaskCompletionHandler;
import org.dcache.replication.v3.CellStubFactory;
import org.dcache.replication.v3.ReplicaManagerTaskExecutor;
import org.dcache.replication.v3.namespace.PoolInfoForPnfsIdTaskHandler;
import org.dcache.replication.v3.namespace.ResilientInfoRegistry;
import org.dcache.replication.v3.namespace.tasks.ReplicaJobDefinition;
import org.dcache.replication.v3.namespace.tasks.ResilientFileInfoTask;
import org.dcache.replication.v3.namespace.tasks.ResilientPoolInfoTask;
import org.dcache.replication.v3.vehicles.ResilientFileInfoMessage;
import org.dcache.replication.v3.vehicles.ResilientPoolInfo;
import org.dcache.replication.v3.vehicles.ResilientPoolInfoForPnfsId;
import org.dcache.vehicles.PnfsSetFileAttributes;


/**
 * Receives {@link PnfsSetFileAttribute} messages and executes
 * {@link ResilientFileInfoTask} tasks.
 *
 * @author arossi
 */
public final class PnfsSetFileAttributesHandler implements CellMessageReceiver,
                PoolInfoForPnfsIdTaskHandler, TaskCompletionHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(PnfsSetFileAttributesHandler.class);
    private final String replicaId = "REPLICAMANAGER" + UUID.randomUUID();

    private NameSpaceProvider namespace;
    private ReplicaManagerTaskExecutor poolInfoTaskExecutor;
    private ReplicaManagerTaskExecutor pnfsInfoTaskExecutor;
    private ScheduledExecutorService migrationTaskExecutor;
    private ReplicaManagerTaskExecutor reductionTaskExecutor;
    private CellStub poolMonitor;
    private CellStub poolManager;
    private CellStub pnfsManager;
    private CellStubFactory poolStubFactory;
    private ResilientInfoRegistry registry;
    private boolean useGreedyRequests;

    private AtomicBoolean accept = new AtomicBoolean(false);

    public void messageArrived(PnfsSetFileAttributes message) {
        /*
         * Initial validity checks done on the message queue thread (there
         * should be little overhead).
         */
        if (!acceptMessage("Modify Cache Location", message)) {
            return;
        }

        /*
         * A check of the session id ensures that we avoid cyclical calls to
         * replicate the same pnfsid by processing the setAttribute calls made
         * for each of the copies requested by the replica manager. Only copy
         * operations originating here will carry this id, so messages from
         * other copy operations (e.g. normal p2p) will be handled and not
         * discarded.
         */
        if (CDC.getSession().equals(replicaId)) {
            LOGGER.debug("{} originated with this replica manager ({}); "
                            + "discarding message.", message, replicaId);
            return;
        }

        CDC.setSession(replicaId);

        PnfsId pnfsId = message.getPnfsId();

        // ///////////// XXX I DON'T KNOW IF THIS IS RIGHT
        // //////////////////////
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

        // //////////////////////////////////////////////////////////////////////

        /*
         * Offload request for resilient pool information onto separate thread.
         * Results will be received asynchronously.
         */
        String pool = locations.iterator().next();
        ResilientPoolInfo info = new ResilientPoolInfoForPnfsId(pnfsId,
                                                                pool,
                                                                message.getSubject());
        poolInfoTaskExecutor.execute(new ResilientPoolInfoTask(info, this));
        LOGGER.debug("executed ResilientPoolInfoTask for {}.", pool);
    }

    public void messageArrived(ResilientFileInfoMessage message) {
        /*
         * Check for permanent system sticky bit and either remove the metadata
         * or send the subsequent request to the pool manager. All done on the
         * message queue thread, but this should have little overhead. Request
         * for resilient pool information received asynchronously.
         */
        LOGGER.trace("PnfsSystemStickyInfoMessage messageArrived: {}.", message);
        if (!message.isSystemSticky()) {
            LOGGER.debug("{}; removing {} from map.", message, message.pnfsId);
            registry.remove(message.pnfsId);
            return;
        }

        /*
         * The next step is to issue a migration task with a completion handler.
         * Should the task complete with less confirmed locations than the
         * current number, the excess should be removed.
         */
        ResilientPoolInfo poolInfo = registry.getPoolInfo(message.pnfsId);
        ReplicaJobDefinition jobDefinition
            = ReplicaJobDefinition.create(poolInfo,
                                          registry.getAttributes(message.pnfsId),
                                          useGreedyRequests, poolManager);
        new Task(this,
                 null,
                 pnfsManager,
                 null,
                 migrationTaskExecutor,
                 poolInfo.poolName,
                 null, // maybe we need to return the CacheEntry.
                 jobDefinition).run();
    }

    /**
     * @return
     */
    public PoolSelectionUnit getSelectionUnit() {
        return null;
    }

    public void setNamespace(NameSpaceProvider namespace) {
        this.namespace = namespace;
    }

    private boolean acceptMessage(String message, Object messageObject) {
        LOGGER.trace("************* Replication {}: {}.", message,
                        messageObject);
        if (!accept.get()) {
            LOGGER.trace("Replica Manager message handler is paused, "
                            + "message will be dropped.");
            return false;
        }
        return true;
    }

    public void handleDone(ResilientPoolInfoForPnfsId info) {
        if (!info.isResilient()) {
            LOGGER.debug("{} does not belong to a resilient group; ignoring message for {}.",
                            info.poolName, info.pnfsId);
            registry.remove(info.pnfsId);
            registry.remove(info.poolName);
            return;
        }

        registry.register(info.poolName, new ResilientPoolInfo(info));

        ResilientFileInfoTask task
            = new ResilientFileInfoTask(info.pnfsId,
                                        poolStubFactory.getCellStub(info.poolName),
                                        info.subject,
                                        registry,
                                        namespace);
        pnfsInfoTaskExecutor.execute(task);
        LOGGER.debug("executed ResilientFileInfoTask for {}.", info.pnfsId);
    }

    public void taskCancelled(Task task) {
        // TODO Auto-generated method stub

    }

    public void taskFailed(Task task, String msg) {
        // TODO Auto-generated method stub

    }

    public void taskFailedPermanently(Task task, String msg) {
        // TODO Auto-generated method stub

    }

    public void taskCompleted(Task task) {
        // TODO Auto-generated method stub
        // it is here that we check to see if there are excess copies
    }
}
