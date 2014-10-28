package org.dcache.replication.v3.pool.tasks;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.MissingResourceException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotInCacheException;
import diskCacheV111.util.PnfsId;

import dmg.util.command.DelayedCommand;

import org.dcache.pool.repository.CacheEntry;
import org.dcache.pool.repository.EntryState;
import org.dcache.pool.repository.Repository;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.replication.v3.vehicles.CacheEntryInfoMessage;

/**
 * @author arossi
 */
public class CacheEntryInfoTask extends DelayedCommand {
    private static final long serialVersionUID = -7672056030995563547L;

    private static final Logger LOGGER
        = LoggerFactory.getLogger(CacheEntryInfoTask.class);

    private final CacheEntryInfoMessage message;
    private final Repository repository;

    public CacheEntryInfoTask(CacheEntryInfoMessage message,
                              Repository repository,
                              Executor executor) {
        super(executor);
        this.message = message;
        this.repository = repository;
    }

    @Override
    protected Serializable execute() throws Exception {
        ensureSystemSticky(message.pnfsId);
        return message;
    }

    private void ensureSystemSticky(PnfsId pnfsId) throws CacheException,
                    InterruptedException {
        try {
            EntryState state = waitUntilReady(pnfsId);
            switch (state) {
                case PRECIOUS:
                case CACHED:
                    /*
                     * Force system sticky.
                     */
                    repository.setSticky(pnfsId,
                                         "system",
                                         StickyRecord.NON_EXPIRING,
                                         true);
                    CacheEntry entry = repository.getEntry(pnfsId);
                    message.setEntry(entry);
                    LOGGER.debug("{}, state {}, entry {}", pnfsId, state, entry);
                    break;
                default:
                    LOGGER.debug("{}, state {}", pnfsId, state);
                    break;
            }
        } catch (FileNotInCacheException e) {
            LOGGER.debug("{} was not in the repository of {}", pnfsId,
                                                               repository.getPoolName());
        }
    }

    private EntryState waitUntilReady(PnfsId pnfsId) throws CacheException,
                    InterruptedException {
        EntryState state = null;
        boolean ready = false;
        int attempt = 1;
        do {
            state = repository.getState(pnfsId);
            switch (state) {
                case REMOVED:
                case DESTROYED:
                    throw new MissingResourceException(pnfsId + ": " + state,
                                    repository.getPoolName(), "waitUntilReady");
                case CACHED:
                case PRECIOUS:
                case BROKEN:
                case NEW: // p2p can produce this state
                    ready = true;
                    break;
                case FROM_CLIENT:
                case FROM_STORE:
                case FROM_POOL:
                default:
                    synchronized (this) {
                        try {
                            wait(TimeUnit.SECONDS.toMillis(2));
                        } catch (InterruptedException ie) {
                            LOGGER.debug("waiting for cache entry for {} on {}"
                                            + " interrupted during try no {}",
                                            pnfsId,
                                            repository.getPoolName(),
                                            attempt);
                        }
                    }
                    ++attempt;
            }

            LOGGER.debug("attempt {}, {}, {}, state {}",
                            attempt,
                            pnfsId,
                            repository.getPoolName(),
                            state);

        } while (!ready);
        return state;
    }
}
