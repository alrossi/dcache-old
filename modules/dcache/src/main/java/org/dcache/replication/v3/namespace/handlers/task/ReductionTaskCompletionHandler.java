package org.dcache.replication.v3.namespace.handlers.task;

import org.dcache.replication.v3.namespace.tasks.ReductionTask;

/**
 * @author arossi
 *
 */
public interface ReductionTaskCompletionHandler {
    void taskCompleted(ReductionTask task);

    void taskFailed(ReductionTask task);

    void taskFailedPermanently(ReductionTask task);

    void taskCancelled(ReductionTask task);
}
