package org.dcache.replication.v3.pool.handlers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import diskCacheV111.util.CacheException;

import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.CellMessageSender;

import org.dcache.pool.repository.Repository;
import org.dcache.replication.v3.CDCFixedPoolTaskExecutor;
import org.dcache.replication.v3.pool.tasks.CacheEntryInfoTask;
import org.dcache.replication.v3.vehicles.CacheEntryInfoMessage;

/**
 * @author arossi
 */
public class CacheEntryInfoMessageHandler implements CellMessageReceiver,
                                                     CellMessageSender {
    private static final Logger LOGGER
        = LoggerFactory.getLogger(CacheEntryInfoMessageHandler.class);

    private CellEndpoint endpoint;
    private Repository repository;
    private CDCFixedPoolTaskExecutor executor;

    public void messageArrived(CellMessage message,
                               CacheEntryInfoMessage info)
                    throws CacheException, InterruptedException {
        /*
         * Get cache entry state info.
         * Note that there may be a wait for the entry to appear in registry,
         * so this work needs to be offloaded onto another thread.
         */
        info.setPool(repository.getPoolName());

        try {
            message.revertDirection();
            new CacheEntryInfoTask(info, repository, executor)
                .call()
                .deliver(endpoint, message);
        } catch (RuntimeException t) {
            LOGGER.error("Unexpected error during processing of {}.",
                            info, t);
        } catch (Exception e) {
            LOGGER.error("Error during processing of {}: {}.",
                            info, e.getMessage());
        }
    }

    public void setCellEndpoint(CellEndpoint endpoint) {
       this.endpoint = endpoint;
    }

    public void setExecutor(CDCFixedPoolTaskExecutor executor) {
        this.executor = executor;
    }

    public void setRepository(Repository repository) {
        this.repository = repository;
    }
}
