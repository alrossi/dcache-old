package org.dcache.replication.v3.namespace.tasks;

import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.PnfsId;

import org.dcache.cells.CellStub;
import org.dcache.replication.v3.CDCFixedPoolTaskExecutor;
import org.dcache.replication.v3.namespace.ResilientInfoCache;
import org.dcache.replication.v3.namespace.handlers.task.FileInfoTaskCompletionHandler;
import org.dcache.replication.v3.vehicles.CacheEntryInfoMessage;
import org.dcache.vehicles.FileAttributes;

/**
 * @author arossi
 */
public class FileInfoTask implements Runnable {
    private static final Logger LOGGER
        = LoggerFactory.getLogger(FileInfoTask.class);

    class CacheEntryResultListener implements Runnable {
        public void run() {
            CacheEntryInfoMessage message = null;
            try {
                message = future.get();
            } catch (InterruptedException | ExecutionException t) {
                handler.taskFailed(message, t.getMessage());
                return;
            }

            if (future.isCancelled()) {
                handler.taskCancelled(message, "Future task was cancelled");
            } else {
                handler.taskCompleted(message);
            }
        }
    }

    private final PnfsId pnfsId;
    private final CellStub pool;
    private final ResilientInfoCache cache;
    private final FileInfoTaskCompletionHandler handler;
    private final CDCFixedPoolTaskExecutor executor;
    private ListenableFuture<CacheEntryInfoMessage> future;

    public FileInfoTask(PnfsId pnfsId,
                        CellStub pool,
                        FileInfoTaskCompletionHandler handler,
                        ResilientInfoCache cache,
                        CDCFixedPoolTaskExecutor executor) {
        this.pnfsId = pnfsId;
        this.pool = pool;
        this.handler = handler;
        this.cache = cache;
        this.executor = executor;
    }

    public void run() {
        FileAttributes attributes;
        try {
            attributes = cache.getAttributes(pnfsId);
            if (!attributes.getAccessLatency().equals(AccessLatency.ONLINE)) {
                handler.taskCancelled(null, pnfsId
                                            + " is not ONLINE; ignoring ...");
                return;
            }
        } catch (ExecutionException t) {
            String error = "CacheEntryInfoTask failed "
                            + "for " + pnfsId + "@" +
                            pool.getDestinationPath().getCellName(); // plus t
            handler.taskFailed(null, error);
            return;
        }

        future = pool.send(new CacheEntryInfoMessage(pnfsId));
        future.addListener(new CacheEntryResultListener(), executor);
        LOGGER.debug("Sent CacheEntryInfoMessage for {} to {}",
                        pnfsId,
                        pool.getDestinationPath().getCellName());
    }
}
