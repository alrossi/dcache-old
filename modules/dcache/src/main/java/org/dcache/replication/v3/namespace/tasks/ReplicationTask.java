package org.dcache.replication.v3.namespace.tasks;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;

import org.dcache.cells.CellStub;
import org.dcache.pool.migration.Task;
import org.dcache.pool.migration.TaskCompletionHandler;
import org.dcache.replication.v3.namespace.ResilientInfoCache;
import org.dcache.replication.v3.namespace.data.ReplicaJobDefinition;
import org.dcache.replication.v3.namespace.data.ResilientPoolGroupInfo;
import org.dcache.replication.v3.vehicles.CacheEntryInfoMessage;
import org.dcache.vehicles.FileAttributes;

/**
 * Largely a wrapper for the actual migration {@link Task}; runs
 * on a separate thread because it makes two calls to the cache
 * which may need to reload from either the pool manager or
 * the database.  The final run() call on the migration task
 * executes it on a different (scheduled) queue/service.
 *
 * @author arossi
 */
public class ReplicationTask implements Runnable {
    protected static final Logger LOGGER
        = LoggerFactory.getLogger(ReplicationTask.class);

    private CacheEntryInfoMessage message;
    private TaskCompletionHandler handler;
    private ScheduledExecutorService executor;
    private ResilientInfoCache cache;
    private CellStub poolManager;
    private CellStub pnfsManager;
    private CellStub pinManager;
    private boolean useGreedyRequests;

    public ReplicationTask(CacheEntryInfoMessage message,
                           TaskCompletionHandler handler,
                           ScheduledExecutorService executor,
                           ResilientInfoCache cache,
                           CellStub poolManager,
                           CellStub pnfsManager,
                           CellStub pinManager,
                           boolean useGreedyRequests) {
        this.message = message;
        this.handler = handler;
        this.executor = executor;
        this.cache = cache;
        this.poolManager = poolManager;
        this.pnfsManager = pnfsManager;
        this.pinManager = pinManager;
        this.useGreedyRequests = useGreedyRequests;
    }

    public void run() {
        String pool = message.getPool();

        FileAttributes attributes;
        ResilientPoolGroupInfo poolInfo;

        try {
            attributes = cache.getAttributes(message.pnfsId);
            poolInfo = cache.getPoolGroupInfo(pool);
        } catch (ExecutionException t) {
            LOGGER.error("Fatal error prior to launching actual "
                            + "migration task for {}@{}: {}.",
                            message.pnfsId,
                            pool,
                            t.getMessage());
            return;
        }

        ReplicaJobDefinition jobDefinition
            = ReplicaJobDefinition.create(poolInfo,
                                          attributes,
                                          pool,
                                          useGreedyRequests,
                                          poolManager);
        new Task(handler,
                 null,
                 pnfsManager,
                 pinManager,
                 executor,
                 pool,
                 message.getEntry(),
                 jobDefinition).run();
    }
}
