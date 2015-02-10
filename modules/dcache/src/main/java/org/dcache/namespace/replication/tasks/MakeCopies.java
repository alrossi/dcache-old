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
package org.dcache.namespace.replication.tasks;

import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import diskCacheV111.util.CacheException;
import org.dcache.alarms.AlarmMarkerFactory;
import org.dcache.alarms.PredefinedAlarm;
import org.dcache.namespace.replication.ReplicaManagerHub;
import org.dcache.namespace.replication.data.PnfsIdInfo;
import org.dcache.namespace.replication.data.PoolGroupInfo;
import org.dcache.namespace.replication.data.PoolManagerInfoList;
import org.dcache.namespace.replication.db.LocalNamespaceAccess;
import org.dcache.pool.migration.PoolSelectionStrategy;
import org.dcache.pool.migration.ProportionalPoolSelectionStrategy;
import org.dcache.pool.migration.Task;
import org.dcache.pool.migration.TaskCompletionHandler;
import org.dcache.pool.migration.TaskParameters;
import org.dcache.pool.repository.EntryState;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.util.replication.CellStubFactory;

/**
 * Serves as a wrapper around the actual migration Task which does
 * the heavy lifting.
 * </p>
 * Implements the completion handler API as required by the migration Task.
 * Note that the migration Task is idempotent, so even if no further copies are
 * made, this worker will also consider the migration Task successful.
 * <p/>
 * If a migration task failure is permanent, this task makes a best effort to
 * find another source pool in the group with a copy of the file and to initiate
 * replication there.  Taking this branch will usually only be possible during
 * a replication triggered by a pool status change or watchdog scan
 * (and not during an attempt to replicate a new file).
 * <p/>
 * Failures to find any viable source raise an alarm.
 *
 * Created by arossi on 2/7/15.
 */
public final class MakeCopies extends ReplicaTask
                              implements TaskCompletionHandler {
    private static final PoolSelectionStrategy PROPORTIONAL_SELECTION
                    = new ProportionalPoolSelectionStrategy();

    private static final ImmutableList<StickyRecord> ONLINE_STICKY_RECORD
                    = ImmutableList.of(new StickyRecord("system",
                    StickyRecord.NON_EXPIRING));

    protected static final String ABORT_COPY_MESSAGE
                    = "Failed {} for ({}, source {}) during {}; "
                    + "source pools tried: {}; {}. "
                    + WILL_RETRY_LATER;

    /*
     * Keeps track of potential source pools for the copy task.
     * In the case of migration failure, a new pool might be selected
     * as source after excluding already tried pools.
     */
    private final Set<String> triedSourcePools = new HashSet<>();
    private final String poolGroup;
    private final PnfsIdInfo pnfsIdInfo;

    private CellStubFactory stubFactory;
    private LocalNamespaceAccess access;
    private String source;

    /*
     * This represents the minimal set of locations which the migration task
     * has deemed or promoted to replica status, and which satisfy
     * the replication count requirement.
     */
    private Collection<String> confirmed;

    /*
     *  The actual migration task.
     */
    private Task task;

    public MakeCopies(ReplicaTaskInfo info,
                      String poolGroup,
                      PnfsIdInfo pnfsIdInfo,
                      ReplicaManagerHub hub) {
        super(info, hub);
        this.poolGroup = poolGroup;
        this.pnfsIdInfo = pnfsIdInfo;
        stubFactory = hub.getCellStubFactory();
        access = hub.getAccess();
    }

    @Override
    public boolean cancel() {
        if (isDone()) {
            return true;
        }

        super.cancel();
        if (task != null) {
            task.cancel();
        }

        return isCancelled();
    }

    /*
     *  A few opportunities have been taken to slice
     *  the execution of this task with checks to see if it has been
     *  cancelled or otherwise terminated.
     */
    @Override
    public void run() {
        selectSourcePool();
        fireMigrationTask();
    }

    @Override
    public ReplicaTaskFuture<MakeCopies> launch() {
        runFuture = hub.getMakeCopiesExecutor().submit(this);
        return new ReplicaTaskFuture<>(this);
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName()
                        + "["
                        + pnfsIdInfo.pnfsId
                        + ", "
                        + source
                        + "]<--"
                        + info;
    }

    /*
     *  Shared with ProcessPnfsId
     */
    Collection<String> getConfirmed() {
        return confirmed;
    }

    /*
     *  A total failure here sends an alarm.
     */
    @Override
    protected void failed(Exception e) {
        LOGGER.error(AlarmMarkerFactory.getMarker(PredefinedAlarm.FAILED_REPLICATION,
                                                  info.pnfsId.toString()),
                        ABORT_COPY_MESSAGE,
                        info.type,
                        info.pnfsId,
                        source,
                        "make copies",
                        triedSourcePools,
                        exceptionMessage(e));
        failedAll();
    }

    /*
     * ********************* TaskCompletionHandler API ***********************
     */

    @Override
    public void taskCancelled(Task task) {
        /*
         *  Can only take place through this task future itself.
         *  Just log the cancellation.
         */
        LOGGER.debug("Migration Task {} for {} on {} cancelled.",
                        task.getId(), info.pnfsId, source);
    }

    @Override
    public void taskCompleted(Task task) {
        LOGGER.debug("Migration Task {} for {} on {} completed successfully.",
                        task.getId(), info.pnfsId, source);
        /*
         * TODO we need ConfirmedLocations from the Migration Task
         * TODO as it stands now, the subsequent remove computes
         * TODO removal based on all locations, which is not optimal
         */
        confirmed = Collections.EMPTY_LIST; //task.getConfirmedLocations(); TODO
        completed();
    }

    /*
     * Not really sure whether the migration task will retry itself here or not.
     * If not, this should call taskFailedPermanently.                      TODO
     */
    @Override
    public void taskFailed(Task task, int rc, String msg) {
        LOGGER.warn("Migration task {} for {} on {} failed; waiting...",
                        task.getId(), info.pnfsId, source);
        /*
         * Do nothing here ... should retry itself?
         */
    }

    /*
     * My current interpretation of this is that the migration cannot
     * be achieved from the current source pool, not from any pool.
     * If this is incorrect, we will need to re-arrange the failure logic.  TODO
     */
    @Override
    public void taskFailedPermanently(Task task, int rc, String msg) {
        LOGGER.debug("Migration task {} for {} on {} failed permanently; "
                                        + "looking for another source pool",
                        task.getId(), info.pnfsId, source);

        if (isDone()) {
            return;
        }

        /*
         * Rerun the task excluding current source.
         */
        triedSourcePools.add(source);
        runFuture = hub.getMakeCopiesExecutor().submit(this);
    }

    /*
     *  Takes into consideration the current locations as reported by
     *  the namespace, the active pools as reported by the pool monitor,
     *  the pools constituting the group as reported by the pool monitor,
     *  and any locations that have already been tried unsuccessfully as
     *  sources.  It will also exclude the original pool in the case of
     *  pool down messages.
     */
    private void selectSourcePool() {
        try {
            /*
             * Refresh the file locations.
             */
            Collection<String> locations = pnfsIdInfo.refreshLocations(access);

            /*
             * Get list of all active pools.
             */
            Collection<String> active = hub.getPoolInfoCache().findAllActivePools();

            /*
             * When DOWN, the originating pool should not be listed there;
             * this is just a precaution.
             */
            if (info.type == ReplicaTaskInfo.Type.POOL_DOWN) {
                active.remove(info.pool);
            }

            PoolGroupInfo poolGroupInfo =
                            hub.getPoolInfoCache().getPoolGroupInfo(info.pool);
            Set<String> pgPools = poolGroupInfo.getPoolNames();

            /*
             * Prune the location list, removing previously tried, non-active,
             * and double-checking the pool is still in that group.
             */
            for (Iterator<String> it = locations.iterator(); it.hasNext();) {
                String location = it.next();
                if (triedSourcePools.contains(location) ||
                                !active.contains(location) ||
                                !pgPools.contains(location)) {
                    it.remove();
                }
            }

            if (locations.isEmpty()) {
                failed(new Exception(String.format("%s has no other active "
                                + "source than the previously tried locations %s.",
                                info.pnfsId, triedSourcePools)));
            } else {
                /*
                 * We don't need a balancing selection strategy here,
                 * as we are choosing another source from which
                 * to replicate, not an optimal target pool
                 * (even if the source pool is "hot", this is a one-time
                 * read).  We just choose randomly from the remaining pools.
                 */
                source = randomSelector.select(locations);
            }
        } catch (CacheException | ExecutionException | InterruptedException t) {
            failed(t);
        }
    }

    private void fireMigrationTask() {
        TaskParameters taskParameters =
                        new TaskParameters(stubFactory.getPoolStub(source),
                                        stubFactory.getCallbackStub(),
                                        stubFactory.getPinManagerStub(),
                                        hub.getMigrationTaskExecutor(),
                                        PROPORTIONAL_SELECTION,
                                        new PoolManagerInfoList(hub.getPoolManagerPoolInfoCache(),
                                                                poolGroup),
                                        true,  // eager
                                        false, // compute checksum on update
                                        false, // force source mode
                                        pnfsIdInfo.getMinimum()); // LACKS onlyOneCopyPer TODO

        /*
         * Calling run() causes the task to execute via the provided executor.
         * The TaskCompletionHandler methods are invoked upon termination.
         */
        task = new Task(taskParameters,
                        this,
                        source,
                        info.pnfsId,
                        EntryState.CACHED,
                        ONLINE_STICKY_RECORD,
                        Collections.EMPTY_LIST,
                        pnfsIdInfo.getAttributes());

        if (isDone()) {
            return;
        }

        task.run();

        LOGGER.debug("Launched migration task for {} on {}.", info.pnfsId, source);
    }
}
