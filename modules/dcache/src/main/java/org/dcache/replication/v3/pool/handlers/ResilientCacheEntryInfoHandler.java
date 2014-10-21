package org.dcache.replication.v3.pool.handlers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import diskCacheV111.util.CacheException;

import dmg.cells.nucleus.CellMessageReceiver;

import org.dcache.cells.CellStub;
import org.dcache.pool.repository.Repository;
import org.dcache.replication.v3.ReplicaManagerTaskExecutor;
import org.dcache.replication.v3.pool.tasks.CacheEntryInfoTask;
import org.dcache.replication.v3.vehicles.ResilientFileInfoMessage;

/**
 * @author arossi
 */
public class ResilientCacheEntryInfoHandler implements CellMessageReceiver {
    private static final Logger LOGGER
        = LoggerFactory.getLogger(ResilientCacheEntryInfoHandler.class);

    private CellStub namespace;
    private Repository repository;
    private ReplicaManagerTaskExecutor executor;

    public void messageArrived(ResilientFileInfoMessage message)
                    throws CacheException, InterruptedException {
        /*
         * Get cache entry state info.
         * Note that there may be a wait for the entry to appear in registry,
         * so this work needs to be offloaded onto another thread.
         */
        executor.execute(new CacheEntryInfoTask(message, repository, namespace));
        LOGGER.debug("Executed CacheEntryInfoTask for {} on {}.", message.pnfsId,
                                                                  repository.getPoolName());
    }

    public void setNamespace(CellStub namespace) {
        this.namespace = namespace;
    }

    public void setRepository(Repository repository) {
        this.repository = repository;
    }
}
