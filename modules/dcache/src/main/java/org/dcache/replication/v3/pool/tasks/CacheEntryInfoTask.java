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
 * Waits for the cache entry object to appear, then fetches it.
 * This task is specific to servicing the replica manager request,
 * so it also sets the system sticky flag if it is not already
 * set.  This is to guarantee consistency on resilient pools
 * (no cached copies without the permanent pin are allowed).
 *
 * @author arossi
 */
public class CacheEntryInfoTask extends DelayedCommand {
    private static final long serialVersionUID = -7672056030995563547L;
    private static final Logger LOGGER
        = LoggerFactory.getLogger(CacheEntryInfoTask.class);
    private static final String SYSTEM_OWNER = "system";

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
