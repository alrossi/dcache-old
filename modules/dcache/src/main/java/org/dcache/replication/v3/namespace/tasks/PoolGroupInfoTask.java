package org.dcache.replication.v3.namespace.tasks;

import java.util.concurrent.ExecutionException;

import diskCacheV111.util.PnfsId;

import org.dcache.replication.v3.namespace.ResilientInfoCache;
import org.dcache.replication.v3.namespace.handlers.task.PoolGroupInfoTaskCompletionHandler;

/**
 * @author arossi
 */
public class PoolGroupInfoTask implements Runnable {
    public final String pool;
    public final PnfsId pnfsId;
    private final PoolGroupInfoTaskCompletionHandler handler;
    private final ResilientInfoCache cache;

    public PoolGroupInfoTask(PnfsId pnfsId,
                             String pool,
                             ResilientInfoCache cache,
                             PoolGroupInfoTaskCompletionHandler handler) {
        this.pool = pool;
        this.pnfsId = pnfsId;
        this.cache = cache;
        this.handler = handler;
    }

    public PoolGroupInfoTask(String pool,
                                 ResilientInfoCache cache,
                                 PoolGroupInfoTaskCompletionHandler handler) {
        this.pool = pool;
        this.pnfsId = null;
        this.cache = cache;
        this.handler = handler;
    }

    public void run() {
        try {
            handler.taskCompleted(pnfsId, pool, cache.getPoolGroupInfo(pool));
        } catch (ExecutionException t) {
            handler.taskFailed(pnfsId, pool, t);
        }
    }
}
