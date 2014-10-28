package org.dcache.replication.v3.namespace.handlers.task;

import org.dcache.replication.v3.namespace.tasks.ReductionTask;

/**
 * @author arossi
 */
public class ReductionTaskCompletionHandler {
    public void taskCompleted(ReductionTask task){}

    public void taskFailed(ReductionTask task, String message){}

    public void taskCancelled(ReductionTask task, String message){}
}
