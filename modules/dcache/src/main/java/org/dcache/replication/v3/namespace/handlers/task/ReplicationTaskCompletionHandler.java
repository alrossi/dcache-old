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

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import diskCacheV111.util.PnfsId;

import org.dcache.alarms.AlarmMarkerFactory;
import org.dcache.alarms.PredefinedAlarm;
import org.dcache.pool.migration.Task;
import org.dcache.pool.migration.TaskCompletionHandler;
import org.dcache.replication.v3.CDCFixedPoolTaskExecutor;
import org.dcache.replication.v3.namespace.ResilientInfoCache;
import org.dcache.replication.v3.namespace.tasks.ReductionTask;
import org.dcache.vehicles.FileAttributes;

/**
 * As required by the migration Task API.
 * <p>
 * If the task succeeds, initiates a post-processing task run on a
 * separate thread pool to eliminate any excess replicas.
 * <p>
 * If the task failure is not permanent, makes a best effort to find
 * another pool in the group with a copy of the file and initiate replication
 * there.  Taking this branch will usually only be possible during a replication
 * triggered by a pool status change or watchdog scan (and not during the
 * original attempt to replicate a new file).
 * <p>
 * Permanent failures raise an alarm.
 *
 * @author arossi
 */
public final class ReplicationTaskCompletionHandler implements TaskCompletionHandler {
    private static final Logger LOGGER
        = LoggerFactory.getLogger(ReplicationTaskCompletionHandler.class);

    private final Set<String> triedSourcePools;
    private final ResilientInfoCache cache;
    private final CDCFixedPoolTaskExecutor reductionTaskExecutor;
    private final ReductionTaskCompletionHandler reductionTaskHandler;
    private final PoolGroupInfoTaskCompletionHandler poolGroupInfoTaskHandler;

    public ReplicationTaskCompletionHandler(Set<String> triedSourcePools,
                                            ResilientInfoCache cache,
                                            CDCFixedPoolTaskExecutor reductionTaskExecutor,
                                            ReductionTaskCompletionHandler reductionTaskHandler,
                                            PoolGroupInfoTaskCompletionHandler poolGroupInfoTaskHandler) {
        this.triedSourcePools = Preconditions.checkNotNull(triedSourcePools);
        this.cache = Preconditions.checkNotNull(cache);
        this.reductionTaskExecutor = Preconditions.checkNotNull(reductionTaskExecutor);
        this.reductionTaskHandler = Preconditions.checkNotNull(reductionTaskHandler);
        this.poolGroupInfoTaskHandler = Preconditions.checkNotNull(poolGroupInfoTaskHandler);
    }

    public void taskCancelled(Task task) {
        LOGGER.warn("Migration task {} for {} was cancelled", task.getId(),
                                                               task.getPnfsId());
    }

    public void taskFailed(Task task, String msg) {
        try {
            PnfsId pnfsId = task.getPnfsId();
            LOGGER.debug("Migration task {} for {} failed; looking for another"
                            + " source pool", task.getId(), pnfsId);
            FileAttributes attributes = cache.getAttributes(pnfsId);
            Collection<String> locations = attributes.getLocations();
            locations.removeAll(triedSourcePools);
            if (locations.isEmpty()) {
                LOGGER.debug("{} has no other potential source than the "
                                + "previously tried locations {}",
                                task.getPnfsId(), triedSourcePools);
                taskFailedPermanently(task, msg);
            } else {
                /*
                 * We don't need a selection strategy here,
                 * as we are choosing another source from which
                 * to replicate, not an optimal target pool
                 * (even if the source pool is "hot", this is a one-time
                 * read).  We just choose the first location.
                 */
                String newSource = locations.iterator().next();
                poolGroupInfoTaskHandler.taskCompleted(pnfsId,
                                                       newSource,
                                                       cache.getPoolGroupInfo(newSource),
                                                       triedSourcePools);

            }
        } catch (ExecutionException t) {
            taskFailedPermanently(task, msg);
        }
    }

    public void taskFailedPermanently(Task task, String msg) {
        /*
         * Send an alarm.
         */
        LOGGER.error(AlarmMarkerFactory.getMarker(PredefinedAlarm.FAILED_REPLICATION,
                                                  task.getPnfsId().toString()),
                        "Failed to replicate {}; tried pools {}",
                            task.getPnfsId(),
                            triedSourcePools);
    }

    public void taskCompleted(Task task) {
        /*
         * Post process the task for excess copies.
         */
        reductionTaskExecutor.execute(new ReductionTask(task.getPnfsId(),
                                                        null, // task.getConfirmedLocations(),
                                                        cache,
                                                        reductionTaskHandler));
    }
}
