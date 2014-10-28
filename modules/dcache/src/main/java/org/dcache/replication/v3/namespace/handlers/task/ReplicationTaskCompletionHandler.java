package org.dcache.replication.v3.namespace.handlers.task;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import diskCacheV111.util.PnfsId;

import org.dcache.pool.migration.Task;
import org.dcache.pool.migration.TaskCompletionHandler;
import org.dcache.replication.v3.CDCFixedPoolTaskExecutor;
import org.dcache.replication.v3.namespace.ResilientInfoCache;
import org.dcache.replication.v3.namespace.tasks.ReductionTask;
import org.dcache.vehicles.FileAttributes;

/**
 * @author arossi
 *
 */
public class ReplicationTaskCompletionHandler implements TaskCompletionHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicationTaskCompletionHandler.class);

    private ResilientInfoCache cache;
    private CDCFixedPoolTaskExecutor reductionTaskExecutor;
    private ReductionTaskCompletionHandler reductionTaskHandler;
    private final Set<String> triedSourcePools;
    private PoolGroupInfoTaskCompletionHandler poolGroupInfoTaskHandler;

    public ReplicationTaskCompletionHandler(Set<String> triedSourcePools) {
        this.triedSourcePools = Preconditions.checkNotNull(triedSourcePools);
    }

    public void taskCancelled(Task task) {
        LOGGER.debug("Migration task {} for {} was cancelled", task.getId(),
                                                               task.getPnfsId());
    }

    public void taskFailed(Task task, String msg) {
        try {
            PnfsId pnfsId = task.getPnfsId();
            LOGGER.debug("Migration task {} for {} failed; looking for another"
                            + " source pool", task.getId(), pnfsId);
            FileAttributes attributes = cache.getAttributes(pnfsId);
            Collection<String> locations = attributes.getLocations();
            locations.removeAll(triedSourcePools);
            if (locations.isEmpty()) {
                LOGGER.debug("{} has no other replicas than {}",
                                task.getPnfsId(), triedSourcePools);
                taskFailedPermanently(task, msg);
                /*
                 * We don't need a selection strategy here,
                 * as we are choosing another source from which
                 * to replicate, not an optimal target pool.
                 * Just take the first.
                 */
                String newSource = locations.iterator().next();
                poolGroupInfoTaskHandler.taskCompleted(pnfsId,
                                                       newSource,
                                                       cache.getPoolGroupInfo(newSource),
                                                       triedSourcePools);

            }
        } catch (ExecutionException t) {
            taskFailedPermanently(task, msg);
        }
    }

    public void taskFailedPermanently(Task task, String msg) {
        /*
         * Send an alarm.
         */
    }

    public void taskCompleted(Task task) {
        /*
         * Post process the task for excess copies.
         */
        reductionTaskExecutor.execute(new ReductionTask(task.getPnfsId(),
                                                        null, // task.getConfirmedLocations(),
                                                        cache,
                                                        reductionTaskHandler));
    }

}
