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

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import org.dcache.pool.classic.PoolV4;
import org.dcache.pool.repository.IllegalTransitionException;
import org.dcache.pool.repository.MetaDataRecord;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.util.CacheExceptionFactory;
import org.dcache.util.ExceptionMessage;
import org.dcache.vehicles.resilience.EnsureSystemStickyBitMessage;
import org.dcache.vehicles.resilience.GetPoolModeMessage;
import org.dcache.vehicles.resilience.GetPoolTagsMessage;
import org.dcache.vehicles.resilience.RemoveReplicaMessage;
import org.dcache.vehicles.resilience.ResilienceStickyBitMessage;

import static diskCacheV111.util.CacheException.PERMISSION_DENIED;

/**
 * <p>Provides API against the repository for tasks specific to the
 * resilience system's message handler running on the pool.
 * Gives direct access to a package remove method which
 * is not exposed through the public API for the repository.</p>
 *
 * Created by arossi on 1/13/15.
 */
public final class RepositoryResilienceProxy {
    private static final Logger LOGGER
        = LoggerFactory.getLogger(RepositoryResilienceProxy.class);

    /*
     *  Owners of sticky records.
     */
    private static final String SYSTEM_OWNER = "system";
    private static final String REPLICA_SYSTEM_OWNER = "replica-system";

    /*
     *  Declaration of the implementation type
     *  is required for the package call to remove().
     */
    private CacheRepositoryV5 repository;
    private PoolV4 pool;

    private int      replicaStickyExpiry     = 5;
    private TimeUnit replicaStickyExpiryUnit = TimeUnit.MINUTES;

    /**
     * Checks to see that there is a sticky record owned by Replica Manager.
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
            if (record.owner()
                      .equals(RepositoryResilienceProxy.REPLICA_SYSTEM_OWNER)) {
                String message = String.format("%s is currently pinned on %s"
                                                               + " by the replica manager; "
                                                               + "this means that it is "
                                                               + "in the process of "
                                                               + "removing one or more copies.",
                                               entry.getPnfsId(), pool);
                throw CacheExceptionFactory.exceptionOf(PERMISSION_DENIED,
                                                        message);
            }
        }
    }

    /**
     * Adds a record in order to pin the file against potential removal by
     * another source.  Only done before the replica manager itself
     * removes one or more copies of a file.
     *
     * @param message requesting addition of sticky record.
     */
    public void addReplicaStickyRecord(ResilienceStickyBitMessage message)
                    throws CacheException, InterruptedException {
        long expiry = System.currentTimeMillis()
                        + replicaStickyExpiryUnit.toMillis(replicaStickyExpiry);
        repository.setSticky(message.pnfsId, REPLICA_SYSTEM_OWNER, expiry,
                             true);
    }

    /**
     * This is a recovery procedure made available via the admin interface.
     * It is invoked when a ReplicationStickyBitMessage arrives without
     * a pnfsId specified.
     */
    public void clearAllReplicaStickyRecords() {
        for (Iterator<PnfsId> pnfsids = repository.iterator(); pnfsids.hasNext(); ) {
            PnfsId pnfsId = pnfsids.next();
            try {
                Collection<StickyRecord> records = repository.getEntry(pnfsId)
                                                             .getStickyRecords();
                for (StickyRecord record : records) {
                    if (record.owner().equals(REPLICA_SYSTEM_OWNER)) {
                        repository.setSticky(pnfsId, REPLICA_SYSTEM_OWNER, 0,
                                             true);
                        break;
                    }
                }
            } catch (CacheException | InterruptedException e) {
                LOGGER.error("Could not remove replica sticky record for {}: {}.",
                             pnfsId, e.getMessage());
            }
        }
    }

    /**
     * Forces the system sticky flag, in order to guarantee
     * consistency on resilient pools (no copies without
     * the permanent pin are allowed).
     *
     * @param message sent by replication system when processing new files.
     *
     * @throws InterruptedException
     * @throws CacheException
     */
    public void ensureSystemSticky(EnsureSystemStickyBitMessage message)
                    throws CacheException, InterruptedException {
        if (message.pnfsId != null) {
            repository.setSticky(message.pnfsId,
                                 SYSTEM_OWNER,
                                 StickyRecord.NON_EXPIRING,
                                 true);
            return;
        }
        setAllSystemSticky();
    }

    public String getPoolName() {
        return repository.getPoolName();
    }

    /**
     * Bypasses the normal setState() call which checks for
     * a sticky record owned by the replica manager.  The special
     * package method forces the removal of the entry.  It is
     * assumed that this call will only be made AFTER all replicas
     * of a pnfsId have been so pinned by the replica system.
     *
     * @param message sent by replica manager requesting removal of copy.
     */
    public void remove(RemoveReplicaMessage message)
                    throws InterruptedException, CacheException,
                    IllegalTransitionException {
        LOGGER.debug("Removing {} from {}.", message.pnfsId, getPoolName());
        repository.remove(message.pnfsId);
    }

    /**
     * Removes record in order to unpin the file.
     * Usually done after the replica manager itself
     * has removed one or more copies of a file.
     *
     * @param message requesting removal of sticky record.
     */
    public void removeReplicaStickyRecord(ResilienceStickyBitMessage message)
                    throws CacheException, InterruptedException {
        repository.setSticky(message.pnfsId, REPLICA_SYSTEM_OWNER, 0, true);
    }

    public void setPool(PoolV4 pool) {
        this.pool = pool;
    }

    public void setPoolMode(GetPoolModeMessage message) {
        message.setMode(pool.getPoolMode());
    }

    public void setPoolTags(GetPoolTagsMessage message) {
        message.setTags(pool.getTagMap());
    }

    public void setReplicaStickyExpiry(int replicaStickyExpiry) {
        this.replicaStickyExpiry = replicaStickyExpiry;
    }

    public void setReplicaStickyExpiryUnit(TimeUnit replicaStickyExpiryUnit) {
        this.replicaStickyExpiryUnit = replicaStickyExpiryUnit;
    }

    public void setRepository(CacheRepositoryV5 repository) {
        this.repository = repository;
    }

    private void setAllSystemSticky() {
        repository.iterator().forEachRemaining((pnfsid) -> {
            try {
                repository.setSticky(pnfsid,
                                     SYSTEM_OWNER,
                                     StickyRecord.NON_EXPIRING,
                                     true);
            } catch (CacheException | InterruptedException e) {
                LOGGER.debug("Could not ensure system sticky record "
                                                + "for {} on {}; {}.",
                                pnfsid, getPoolName(),
                                new ExceptionMessage(e));
            }
        });
    }
}
