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
package org.dcache.replication.v3.namespace.handlers.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import diskCacheV111.util.PnfsId;

import org.dcache.replication.v3.CellStubFactory;
import org.dcache.replication.v3.namespace.ReplicaManagerHub;
import org.dcache.replication.v3.namespace.data.PoolGroupInfo;
import org.dcache.replication.v3.namespace.tasks.FileInfoTask;

/**
 * Initiates the next stage of the replica processing if the
 * source pool is resilient (otherwise it simply returns).
 * <p>
 * If the pool belongs to a resilient group, then the source's
 * CacheEntry object if fetched from the pool repository.  That
 * call also ensures that this copy has a sticky bit set belonging
 * to "system" and of indefinite expiration.  That is, the replica
 * manager DOES NOT allow temporary (simply cached) copies of files
 * to reside on resilient pools.  The reason for this is to facilitate
 * the handling of pool status checks (maintaining this consistency
 * renders the verification much more efficient).
 * <p>
 * The call to the pool repository is handled by a separate task on
 * a new thread.
 * <p>
 * This handler has no state to maintain and thus is reusable.
 *
 * @author arossi
 */
public final class PoolGroupInfoTaskCompletionHandler {
    private static final Logger LOGGER
        = LoggerFactory.getLogger(PoolGroupInfoTaskCompletionHandler.class);

    private ReplicaManagerHub hub;

    public void setHub(ReplicaManagerHub hub) {
        this.hub = hub;
    }

    public void taskCompleted(PnfsId pnfsId,
                              String pool,
                              PoolGroupInfo info,
                              Set<String> triedPools) {
        if (!info.isResilient()) {
            LOGGER.debug("{} does not belong to a resilient group; "
                            + "discarding message for {}.", pool, pnfsId);
            return;
        }

        /*
         * The next step is to ensure the file in this location is actually
         * CACHED+system. The task first gets the file attributes (potentially a
         * database call), then sends an asynchronous message to the pool,
         * enforcing the sticky bit and returning the cache entry object to pass
         * on to the migration task.
         */
        FileInfoTask task = new FileInfoTask(pnfsId,
                                             pool,
                                             hub,
                                             triedPools);
        hub.getPnfsInfoTaskExecutor().execute(task);
        LOGGER.debug("executed FileInfoTask for {}.", pnfsId);
    }

    /*
     *   TODO:  We may want to change this policy here to an immediate retry.
     */
    public void taskFailed(PnfsId pnfsId, String pool, String message) {
        LOGGER.error("Verifying pool group information for {}@{} failed: {}.  "
                        + "Replication cannot proceed at this time, but will "
                        + "be retried during the next periodic watchdog scan.",
                        pnfsId, pool, message);
    }
}
