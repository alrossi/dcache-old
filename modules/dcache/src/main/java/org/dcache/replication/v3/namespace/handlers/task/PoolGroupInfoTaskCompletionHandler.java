package org.dcache.replication.v3.namespace.handlers.task;

import diskCacheV111.util.PnfsId;

import org.dcache.replication.v3.namespace.data.ResilientPoolGroupInfo;

/**
 * @author arossi
 *
 */
public interface PoolGroupInfoTaskCompletionHandler {
    void taskCompleted(PnfsId pnfsId, String pool, ResilientPoolGroupInfo info);

    void taskFailed(PnfsId pnfsId, String pool, Exception exception);
}
