/*
COPYRIGHT STATUS:
Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
software are sponsored by the U.S. Department of Energy under Contract No.
DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
non-exclusive, royalty-free license to publish or reproduce these documents
and software for U.S. Government purposes.  All documents and software
available from this server are protected under the U.S. and Foreign
Copyright Laws, and FNAL reserves all rights.

Distribution of the software available from this server is free of
charge subject to the user following the terms of the Fermitools
Software Legal Information.

Redistribution and/or modification of the software shall be accompanied
by the Fermitools Software Legal Information  (including the copyright
notice).

The user is asked to feed back problems, benefits, and/or suggestions
about the software to the Fermilab Software Providers.

Neither the name of Fermilab, the  URA, nor the names of the contributors
may be used to endorse or promote products derived from this software
without specific prior written permission.

DISCLAIMER OF LIABILITY (BSD):

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED  WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED  WARRANTIES OF MERCHANTABILITY AND FITNESS
FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL FERMILAB,
OR THE URA, OR THE U.S. DEPARTMENT of ENERGY, OR CONTRIBUTORS BE LIABLE
FOR  ANY  DIRECT, INDIRECT,  INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
OF SUBSTITUTE  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY  OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT  OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE  POSSIBILITY OF SUCH DAMAGE.

Liabilities of the Government:

This software is provided by URA, independent from its Prime Contract
with the U.S. Department of Energy. URA is acting independently from
the Government and in its own private capacity and is not acting on
behalf of the U.S. Government, nor as its contractor nor its agent.
Correspondingly, it is understood and agreed that the U.S. Government
has no connection to this software and in no manner whatsoever shall
be liable for nor assume any responsibility or obligation for any claim,
cost, or damages arising out of or resulting from the use of the software
available from this server.

Export Control:

All documents and software available from this server are subject to U.S.
export control laws.  Anyone downloading information from this server is
obligated to secure any necessary Government licenses before exporting
documents or software obtained from this server.
 */
package org.dcache.pool.repository.v5;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.MissingResourceException;
import java.util.concurrent.TimeUnit;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import org.dcache.pool.repository.CacheEntry;
import org.dcache.pool.repository.EntryState;
import org.dcache.pool.repository.IllegalTransitionException;
import org.dcache.pool.repository.MetaDataRecord;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.util.CacheExceptionFactory;
import org.dcache.vehicles.replication.CacheEntryInfoMessage;
import org.dcache.vehicles.replication.RemoveReplicaMessage;
import org.dcache.vehicles.replication.StickyReplicaMessage;

import static diskCacheV111.util.CacheException.DEFAULT_ERROR_CODE;
import static diskCacheV111.util.CacheException.PERMISSION_DENIED;

/**
 * Provides API against the repository for tasks specific to the
 * replica manager message handler running on the pool.
 * Gives direct access to a package remove method which is not exposed through
 * the public API for the repository.
 *
 * Created by arossi on 1/13/15.
 */
public final class ReplicaManagerRepositoryProxy {
    private static final Logger LOGGER
        = LoggerFactory.getLogger(ReplicaManagerRepositoryProxy.class);

    /*
     *  Owners of sticky records.
     */
    private static final String SYSTEM_OWNER = "system";
    private static final String REPLICA_MANAGER_OWNER = "replica-manager";

    /**
     * Checks to see that there is no sticky record owned by Replica Manager.
     * Throws exception if there is.  (Called from the Repository when
     * doing setState()).
     *
     * @param entry of the replica
     * @param pool on which the entry is found
     *
     * @throws CacheException
     */
    static void assertNoReplicaStickyRecord(MetaDataRecord entry, String pool)
                    throws CacheException {
        for (StickyRecord record : entry.stickyRecords()) {
            if (record.owner().equals(ReplicaManagerRepositoryProxy.REPLICA_MANAGER_OWNER)) {
                String message = String.format("%s is currently pinned"
                                                + " by the replica manager; "
                                                + "this means that it is "
                                                + "in the process of "
                                                + "being removed from %s.",
                                entry.getPnfsId(), pool);
                throw CacheExceptionFactory
                                .exceptionOf(PERMISSION_DENIED, message);
            }
        }
    }

    /*
     *  Implementation type required for package call to remove().
     */
    private CacheRepositoryV5 repository;
    private int cacheWait = 2;
    private TimeUnit cacheWaitUnit = TimeUnit.SECONDS;
    private int cacheMaxRetries = 10;
    private int replicaStickyExpiry = 12;
    private TimeUnit replicaStickyExpiryUnit = TimeUnit.HOURS;

    public String getPoolName() {
        return repository.getPoolName();
    }

    /**
     * Bypasses the normal setState() call which checks for
     * a sticky record owned by the replica manager.  The special
     * package method forces the removal of the entry.  It is
     * assumed that this call will only be made AFTER all replicas
     * of a pnfsId have been so pinned by the replica manager.
     *
     * @param message sent by replica manager requesting removal of copy/ies.
     */
    public void remove(RemoveReplicaMessage message)
                    throws InterruptedException, CacheException,
                    IllegalTransitionException {
        repository.remove(message.pnfsId);
    }

    /**
     * Waits for the cache entry object to appear, then fetches it.
     * This task also sets the system sticky flag if it is not already
     * set, in order to guarantee consistency on resilient pools
     * (no cached copies without the permanent pin are allowed).
     *
     * @param message sent by replica manager requesting cache info.
     *
     * @throws InterruptedException
     * @throws CacheException
     */
    public void ensureSystemSticky(CacheEntryInfoMessage message)
                    throws CacheException, InterruptedException {
         PnfsId pnfsId = message.pnfsId;
         EntryState state = waitUntilReady(pnfsId);
         switch (state) {
             case PRECIOUS:
             case CACHED:
                 /*
                  * Force system-sticky.
                  */
                 repository.setSticky(pnfsId,
                                      SYSTEM_OWNER,
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
    }

    /**
     * Adds record in order to pin the file against potential removal by
     * another source.  Usually done before the replica manager itself
     * removes one or more copies of a file.
     *
     * @param message requesting addition of sticky record
     */
    public void addReplicaManagerStickyRecord(StickyReplicaMessage message)
                    throws CacheException, InterruptedException {
        repository.setSticky(message.pnfsId,
                             REPLICA_MANAGER_OWNER,
                             System.currentTimeMillis()
                             + replicaStickyExpiryUnit.toMillis(replicaStickyExpiry),
                             true);
    }

    /**
     * Removes record in order to unpin the file.
     * Usually done after the replica manager itself
     * has removed one or more copies of a file.
     *
     * @param message requesting removal of sticky record
     */
    public void removeReplicaManagerStickyRecord(StickyReplicaMessage message)
                    throws CacheException, InterruptedException {
        repository.getEntry(message.pnfsId)
                  .getStickyRecords().removeIf(
                        (record) -> record.owner().equals(REPLICA_MANAGER_OWNER));
    }

    public void setRepository(CacheRepositoryV5 repository) {
        this.repository = repository;
    }

    public void setCacheWait(int cacheWait) {
        this.cacheWait = cacheWait;
    }

    public void setCacheWaitUnit(TimeUnit cacheWaitUnit) {
        this.cacheWaitUnit = cacheWaitUnit;
    }

    public void setCacheMaxRetries(int cacheMaxRetries) {
        this.cacheMaxRetries = cacheMaxRetries;
    }

    public void setReplicaStickyExpiry(int replicaStickyExpiry) {
        this.replicaStickyExpiry = replicaStickyExpiry;
    }

    public void setReplicaStickyExpiryUnit(TimeUnit replicaStickyExpiryUnit) {
        this.replicaStickyExpiryUnit = replicaStickyExpiryUnit;
    }

    /*
     *  Waits until the entry state is CACHED, PRECIOUS, BROKEN or NEW.
     */
    private EntryState waitUntilReady(PnfsId pnfsId) throws CacheException,
                    InterruptedException {
        EntryState state;
        boolean ready = false;
        int attempt = 1;
        do {
            state = repository.getState(pnfsId);
            switch (state) {
                case REMOVED:
                case DESTROYED:
                    throw new MissingResourceException(pnfsId + ": " + state,
                                    repository.getPoolName(),
                                    "waitUntilReady");
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
                    if (attempt >= cacheMaxRetries) {
                        String message = String.format("Entry state of %s still "
                                        + "%s after %s attempts; aborting.",
                                        pnfsId, state, attempt);
                        throw CacheExceptionFactory.exceptionOf(DEFAULT_ERROR_CODE,
                                        message);
                    }

                    synchronized (this) {
                        try {
                            wait(cacheWaitUnit.toMillis(cacheWait));
                        } catch (InterruptedException ie) {
                            LOGGER.debug("waiting for cache entry for {} on {}"
                                          + " interrupted during attempt no {}",
                                            pnfsId,
                                            repository.getPoolName(),
                                            attempt);
                        }
                    }

                    ++attempt;
            }
        } while (!ready);

        return state;
    }
}
