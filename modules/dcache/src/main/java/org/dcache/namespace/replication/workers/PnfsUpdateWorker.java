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
package org.dcache.namespace.replication.workers;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import org.dcache.alarms.AlarmMarkerFactory;
import org.dcache.alarms.PredefinedAlarm;
import org.dcache.namespace.replication.ReplicaManagerHub;
import org.dcache.namespace.replication.data.PnfsIdInfo;
import org.dcache.namespace.replication.data.PoolManagerInfoList;
import org.dcache.pool.migration.PoolSelectionStrategy;
import org.dcache.pool.migration.ProportionalPoolSelectionStrategy;
import org.dcache.pool.migration.Task;
import org.dcache.pool.migration.TaskCompletionHandler;
import org.dcache.pool.migration.TaskParameters;
import org.dcache.pool.replication.vehicles.CacheEntryInfoMessage;
import org.dcache.pool.repository.EntryState;
import org.dcache.pool.repository.StickyRecord;

/**
 * A worker responsible for all phases of the replication task for
 * a single pnfsid.
 * <p/>
 * Queues itself to run on the appropriate queues for each phase.
 * <p/>
 * A check is first done to make sure the source pool belongs to a resilient
 * group.  The pool group information is retrieved via a cache based on a periodic
 * refresh of the pool monitor. Similarly, file attributes are retrieved
 * from a cache loaded from the namespace database.
 * <p/>
 * Implements completion handler API as required by the migration Task.
 * If that task succeeds, initiates post-processing run on a
 * separate thread pool to eliminate any excess replicas.  Note that
 * the migration Task is idempotent, so even if no further copies are
 * made, this worker will subsequently check for excess copies since it
 * will consider the migration Task successful.
 * <p/>
 * If the migration task failure is not permanent, makes a best effort to find
 * another pool in the group with a copy of the file and initiate replication
 * there.  Taking this branch will usually only be possible during a replication
 * triggered by a pool status change or watchdog scan (and not during the
 * original attempt to replicate a new file).
 * <p/>
 * Any redundant copies are removed via a call to the replica manager
 * hook on the pool in question.  The cache entry state is actually set to removed
 * in the repository (as if rep rm -f were called), because of the policy
 * enforced concerning the presence of a sticky record owned by system for
 * all files on resilient pools (if removal were left to the sweeper, the
 * pool would be in an inconsistent state for some interval).  All replicas
 * of the file are first pinned by the replica manager so that any externally
 * initiated removal during this phase will not succeed; those sticky records
 * are removed when the operation completes.
 * <p/>
 * All permanent failures raise an alarm.
 *
 * Created by arossi on 1/22/15.
 */
public final class PnfsUpdateWorker extends AbstractUpdateWorker<PnfsId>
                                    implements TaskCompletionHandler {
    private static final PoolSelectionStrategy PROPORTIONAL_SELECTION
                    = new ProportionalPoolSelectionStrategy();

    private static final ImmutableList<StickyRecord> ONLINE_STICKY_RECORD
                    = ImmutableList.of(new StickyRecord("system",
                                                        StickyRecord.NON_EXPIRING));

    private static final String ABORT_MESSAGE
                    = "Failed to replicate ({}, source {}) during phase {}; "
                    + "source pools tried: {}; "
                    + "exception {}, cause: {}. "
                    + "Replication cannot proceed at this time; a best effort "
                    + "at retry will be made during the next periodic watchdog "
                    + "scan.";

    enum State {
        START,
        POOLGROUP_INFO,     // cache access, possibly calls pool manager
        FILE_INFO,          // cache access, possibly calls chimera
        CACHEENTRY_INFO,    // sends message to source pool
        REPLICATION,        // executes migration task
        REDUCTION,          // executes potential reduction/cleanup
        DONE
    }

    private final PnfsId pnfsId;

    /*
     * Keeps track of potential source pools for the copy task.
     * In the case of migration failure, a new pool might be selected
     * as source after excluding already tried pools.
     */
    private final Set<String> triedSourcePools = new HashSet<>();

    private PnfsIdInfo pnfsIdInfo;
    private Task task;

    /*
     * This represents the minimal set of locations which the migration task
     * has deemed or promoted to replica status, and which satisfy
     * the replication count requirement.
     */
    private Collection<String> confirmed;

    private State state = State.START;

    public PnfsUpdateWorker(String poolName,
                            PnfsId pnfsId,
                            ReplicaManagerHub hub) {
        super(poolName, hub);
        this.pnfsId = pnfsId;
    }

    /**
     * @return pnfsid targeted by this worker.
     */
    public PnfsId getPnfsId() {
        return pnfsId;
    }

    public String getWorkerName() {
        return pnfsId.toString();
    }

    @Override
    public void run() {
        /*
         * NOTE that the REPLICATION logic is different in that the next
         * phase is set by the completion handler.  This is because the
         * migration task queues itself onto a separate scheduled executor.
         */
        switch (state) {
            case START:           register();            nextState(); break;
            case POOLGROUP_INFO:  getPoolGroupInfo();    nextState(); break;
            case FILE_INFO:       getPnfsIdInfo();       nextState(); break;
            case CACHEENTRY_INFO: getCacheEntryInfo();   nextState(); break;
            case REPLICATION:     replicate();                        break;
            case REDUCTION:       reduce();              nextState(); break;
            default:                                                  break;
        }
    }

    /*
     * ********************* TaskCompletionHandler API ***********************
     */

    @Override
    public void taskCancelled(Task task) {
        failed(new Exception(String.format("Migration task %s on %s was cancelled.",
                                            task.getId(), poolName)));
    }

    @Override
    public void taskCompleted(Task task) {
        LOGGER.debug("Migration Task {} for {} on {} completed successfully.",
                        task.getId(), pnfsId, poolName);
        /*
         * TODO we need ConfirmedLocations from the Migration Task
         * TODO as it stands now, all tasks run should report failure
         */
        confirmed = Collections.EMPTY_LIST; //task.getConfirmedLocations(); TODO
        if (confirmed.isEmpty() ) {
            failed(new Exception("Migration task returned no confirmed locations."));
            return;
        }

        nextState();
    }

    /*
     * Not really sure whether the migration task will retry itself here or not.
     * If not, this should call taskFailedPermanently.
     */
    @Override
    public void taskFailed(Task task, int rc, String msg) {
        LOGGER.warn("Migration task {} for {} on {} failed; waiting...",
                        task.getId(), pnfsId, poolName);
        /*
         * Do nothing here ... should retry itself?
         */
    }

    @Override
    public void taskFailedPermanently(Task task, int rc, String msg) {
        try {
            LOGGER.debug("Migration task {} for {} on {} failed permanently; "
                            + "looking for another source pool",
                            task.getId(), pnfsId, poolName);

            /*
             * Refresh the file locations.
             */
            Collection<String> locations = pnfsIdInfo.refreshLocations();
            locations.removeAll(triedSourcePools);

            /*
             * Make sure the source location is in the pool group.
             */
            Set<String> pgPools = poolGroupInfo.getPoolNames();
            for (Iterator<String> it = locations.iterator(); it.hasNext();) {
                String location = it.next();
                if (!pgPools.contains(location)) {
                    it.remove();
                }
            }

            if (locations.isEmpty()) {
                LOGGER.debug("{} has no other potential source than the "
                                                + "previously tried locations {}",
                                task.getPnfsId(), triedSourcePools);
                failed(new Exception(String.format("rc=%s; %s.", rc, msg)));
                done();
            } else {
                /*
                 * We don't need a balancing selection strategy here,
                 * as we are choosing another source from which
                 * to replicate, not an optimal target pool
                 * (even if the source pool is "hot", this is a one-time
                 * read).  We just choose randomly from the remaining pools.
                 */
                poolName = randomSelector.select(locations);

                /*
                 * Since we have chosen from the pool group, we can
                 * reset to that state and proceed to get file attributes
                 * and cache entry pnfsIdInfo for the new source.
                 */
                state = State.POOLGROUP_INFO;
                nextState();
            }
        } catch (CacheException t) {
            failed(t);
            done();
        }
    }

    @Override
    protected void cancel() {
        super.cancel();
        synchronized(this) {
            if (task != null) {
                task.cancel();
            }
        }
    }

    @Override
    protected synchronized void done() {
        state = State.DONE;
        LOGGER.debug("Completed processing of {} on {}.", pnfsId, poolName);

        /*
         * For the sake of any caller holding a future against this worker.
         */
        synchronized (this) {
            notifyAll();
        }

        /*
         *  Cache life needs to be sufficient to allow accessing
         *  during longer scan operations, so the single operations
         *  should clean up after themselves.
         */
        hub.getPnfsInfoCache().invalidate(pnfsId);

        unregister();
    }

    @Override
    protected void failed(Exception e) {
        LOGGER.error(AlarmMarkerFactory.getMarker(PredefinedAlarm.FAILED_REPLICATION,
                                                  pnfsId.toString()),
                        ABORT_MESSAGE,
                        pnfsId,
                        poolName,
                        state,
                        triedSourcePools,
                        e == null ? "" : e.getMessage(),
                        e == null ? "" : String.valueOf(e.getCause()));
        hub.getRegistry().failed(this);
        done();
    }

    @Override
    protected synchronized boolean isDone() {
        return state == State.DONE;
    }

    @Override
    protected synchronized void launch() {
        LOGGER.debug("Launching phase {} for {} on {}.", state, pnfsId, poolName);

        switch (state) {
            case POOLGROUP_INFO:
                running = hub.getPoolGroupInfoTaskExecutor().submit(this);
                break;
            case FILE_INFO:
            case CACHEENTRY_INFO:
                running = hub.getPnfsInfoTaskExecutor().submit(this);
                break;
            case REPLICATION:
                /*
                 * We do the preparation for task execution on
                 * the current thread (pnfsInfoTaskExecutor), since
                 * it is inexpensive. The migration task queues
                 * itself onto a separate scheduled executor.
                 */
                run();
                break;
            case REDUCTION:
                running = hub.getReductionTaskExecutor().submit(this);
                break;
            case DONE:
                /*
                 * Should not get here ...
                 */
                return;
            default:
                String message = String.format("Worker for %s on %s launched in "
                                                + "an illegal state %s.",
                                                pnfsId, poolName, state);
                hub.getRegistry().failed(this);
                throw new IllegalStateException(message);
        }
    }

    @Override
    protected synchronized void nextState() {
        LOGGER.debug("Completed phase {} for {} on {}.", state, pnfsId, poolName);

        running = null;
        task = null;

        switch (state) {
            case START:           state = State.POOLGROUP_INFO;  launch(); break;
            case POOLGROUP_INFO:  state = State.FILE_INFO;       launch(); break;
            case FILE_INFO:       state = State.CACHEENTRY_INFO; launch(); break;
            case CACHEENTRY_INFO: state = State.REPLICATION;     launch(); break;
            case REPLICATION:     state = State.REDUCTION;       launch(); break;
            case REDUCTION:                                      done();   break;
            default:                                                       break;
        }
    }

    /*
     * Compares the confirmed locations returned by the migration task
     * with the full set of active locations registered in chimera, and
     * removes the excess ones.
     */
    @Override
    protected void reduce() {
        List < PnfsId > pnfsIds = new ArrayList<>();
        pnfsIds.add(pnfsId);
        toRemove = ArrayListMultimap.create();
        selectForReduction(pnfsIds, toRemove, confirmed);
        remove();
        unpin(toUnpin.asMap());
    }

    @Override
    protected void register() {
        hub.getRegistry().register(this);
    }

    @Override
    protected void replicate() {
        TaskParameters taskParameters =
                        new TaskParameters(stubFactory.getPoolStub(poolName),
                                           stubFactory.getCallbackStub(),
                                           stubFactory.getPinManagerStub(),
                                           hub.getMigrationTaskExecutor(),
                                           PROPORTIONAL_SELECTION,
                                           new PoolManagerInfoList(hub.getPoolManagerPoolInfoCache(),
                                                                   poolGroupInfo.getPoolGroup().getName()),
                                           true,  // eager
                                           false, // compute checksum on update
                                           false, // force source mode
                                           pnfsIdInfo.getMinimum()); // LACKS onlyOneCopyPer TODO

        triedSourcePools.add(poolName);

        /*
         * Calling run() causes the task to execute via the provided executor.
         * The TaskCompletionHandler methods are invoked upon termination.
         */
        task = new Task(taskParameters,
                        this,
                        poolName,
                        pnfsId,
                        EntryState.CACHED,
                        ONLINE_STICKY_RECORD,
                        Collections.EMPTY_LIST,
                        pnfsIdInfo.getAttributes());
        task.run();
        LOGGER.debug("Launched migration task for {} on {}.", pnfsId, poolName);
    }

    @Override
    protected PnfsId returnValue() {
        return pnfsId;
    }

    private void getCacheEntryInfo() {
        CacheEntryInfoMessage message = new CacheEntryInfoMessage(pnfsId);
        Future<CacheEntryInfoMessage> future
                        = stubFactory.getPoolStub(poolName).send(message);

        LOGGER.trace("Sent CacheEntryInfoMessage {}.", message);

        try {
            /*
             * Should block until ready.
             */
            future.get();
        } catch (InterruptedException | ExecutionException t) {
            failed(t);
            return;
        }

        if (future.isCancelled()) {
            /*
             * The cancellation can occur if the access latency for the
             * file was for some reason not ONLINE.
             */
            LOGGER.warn("Attempt to get cache entry was cancelled: {}.", message);
            done();
        }
    }

    private void getPnfsIdInfo() {
        try {
            pnfsIdInfo = hub.getPnfsInfoCache().getPnfsIdInfo(pnfsId);
            if (!pnfsIdInfo.getAttributes().getAccessLatency()
                     .equals(AccessLatency.ONLINE)) {
                LOGGER.debug("AccessLatency of {} is not ONLINE; ignoring ...",
                                pnfsId);
                done();
            }

            pnfsIdInfo.setConstraints(poolGroupInfo);

            /*
             * Note that we do not check constraints here because
             * the single pnfsId update is based on the Migration Task
             * and presupposes that task will "do the right thing" in terms
             * of which copies should be created and retained.
             */
        } catch (ExecutionException t) {
            failed(t);
        }
    }
}
