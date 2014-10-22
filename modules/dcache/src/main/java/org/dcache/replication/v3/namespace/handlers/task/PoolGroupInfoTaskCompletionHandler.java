package org.dcache.replication.v3.namespace.handlers.task;

import diskCacheV111.util.PnfsId;

import org.dcache.replication.v3.namespace.data.PoolGroupInfo;

/**
 * @author arossi
 */
public interface PoolGroupInfoTaskCompletionHandler {
    void taskCompleted(PnfsId pnfsId, String pool, PoolGroupInfo info);

    void taskFailed(PnfsId pnfsId, String pool, String message);
}
