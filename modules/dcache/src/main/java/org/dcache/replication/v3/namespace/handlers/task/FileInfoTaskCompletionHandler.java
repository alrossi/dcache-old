package org.dcache.replication.v3.namespace.handlers.task;

import org.dcache.replication.v3.vehicles.CacheEntryInfoMessage;

/**
 * @author arossi
 *
 */
public interface FileInfoTaskCompletionHandler {
    void taskCompleted(CacheEntryInfoMessage info);

    void taskFailed(CacheEntryInfoMessage info, Exception exception);

    void taskCancelled(CacheEntryInfoMessage info, String message);
}
