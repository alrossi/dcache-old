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

import java.util.HashSet;
import java.util.Set;

import org.dcache.replication.v3.namespace.ReplicaManagerHub;
import org.dcache.replication.v3.namespace.tasks.ReplicationTask;
import org.dcache.replication.v3.vehicles.CacheEntryInfoMessage;

/**
 * Initiates the next stage of the replica processing if the
 * cache entry info request succeeds.
 * <p>
 * Note that the handler provides for repeated tries using different
 * source pools by forwarding the "tried" accumulator with the current
 * source pool added.  The accumulator is checked by the replication task's
 * completion handler in case of failure; if there are other potential
 * source locations which do not appear in this set of "tried" pools,
 * another attempt at replication will be made.
 * <p>
 * The ReplicationTask is executed on the same executor pool as
 * that used for the cache info task, since after very brief work it
 * then submits the actual migration task onto a different (scheduled)
 * executor as required by that API.
 * <p>
 * This handler has no task-specific state to maintain and thus is reusable.
 *
 * @author arossi
 */
public final class FileInfoTaskCompletionHandler {
    private static final Logger LOGGER
        = LoggerFactory.getLogger(FileInfoTaskCompletionHandler.class);

    private ReplicaManagerHub hub;
    private boolean useGreedyRequests;

    public void setHub(ReplicaManagerHub hub) {
        this.hub = hub;
    }

    public void setUseGreedyRequests(boolean useGreedyRequests) {
        this.useGreedyRequests = useGreedyRequests;
    }

    public void taskCancelled(CacheEntryInfoMessage info, String message) {
        if (info != null) {
            LOGGER.warn(message + ": {}@{}", info.pnfsId, info.getPool());
        } else {
            /*
             * The cancellation can occur if the access latency for the
             * file was for some reason not ONLINE. In this case, the
             * info object is <code>null</code>.
             */
            LOGGER.warn(message);
        }
    }

    public void taskCompleted(CacheEntryInfoMessage info, Set<String> tried) {
        LOGGER.trace("CacheEntryInfoMessage messageArrived: {}.", info);

        if (tried == null) {
            tried = new HashSet<>();
        }

        tried.add(info.getPool());

        /*
         * The replication completion handler needs to maintain
         * the current set of tried pools.  Hence it is not
         * reusable.  A separate instance needs to be passed to
         * each replication task.
         *
         */
        ReplicationTaskCompletionHandler handler =
                        new ReplicationTaskCompletionHandler(tried, hub);
        /*
         * Issue the migration task with a completion handler. Should the task
         * complete with less confirmed locations than the current number, the
         * excess locations will be removed.
         */
        ReplicationTask task = new ReplicationTask(info,
                                                   handler,
                                                   hub,
                                                   useGreedyRequests);
        hub.getPnfsInfoTaskExecutor().execute(task);
    }

    /*
     *   TODO:  We may want to change the policy here to an immediate retry.
     */
    public void taskFailed(CacheEntryInfoMessage info, String message) {
        LOGGER.error("Verifying cache entry information for {}@{} failed: {}.  "
                        + "Replication cannot proceed at this time, but will "
                        + "be retried during the next periodic watchdog scan.",
                        info.pnfsId, info.getPool(), message);
    }
}
