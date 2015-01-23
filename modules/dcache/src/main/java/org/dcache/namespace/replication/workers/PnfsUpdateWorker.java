package org.dcache.namespace.replication.workers;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import org.dcache.namespace.replication.CellStubFactory;
import org.dcache.namespace.replication.ReplicaManagerHub;
import org.dcache.namespace.replication.data.PoolGroupInfo;
import org.dcache.namespace.replication.data.ReplicationTaskParametersFactory;
import org.dcache.pool.migration.Task;
import org.dcache.pool.migration.TaskCompletionHandler;
import org.dcache.pool.migration.TaskParameters;
import org.dcache.pool.repository.EntryState;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.replicamanager.CacheEntryInfoMessage;
import org.dcache.vehicles.replicamanager.RemoveReplicasMessage;
import org.dcache.vehicles.replicamanager.StickyReplicasMessage;

/**
 * A worker responsible for all phases of the replication task for
 * a single pnfsid.
 * <br>
 *
 * Written as a state machine.  Queues itself to run on the appropriate
 * queues for each phase.
 * <br>
 *
 * Implements complete handler as required by the migration Task API.
 * If that task succeeds, initiates a post-processing task run on a
 * separate thread pool to eliminate any excess replicas.
 * <br>
 *
 * If the task failure is not permanent, makes a best effort to find
 * another pool in the group with a copy of the file and initiate replication
 * there.  Taking this branch will usually only be possible during a replication
 * triggered by a pool status change or watchdog scan (and not during the
 * original attempt to replicate a new file).
 * <br>
 *
 * Any redundant copies are removed via a call to the replica manager
 * handler on the pool in question.  The entry is actually removed from
 * the repository (as if rep rm -f were called), because of the policy
 * enforced concerning the presence of a sticky record owned by system for
 * all files on resilient pools (if removal were left to the sweeper, the
 * pool would be in an inconsistent state for some interval).  All replicas
 * of the file are first pinned by the replica manager so that any externally
 * initiated removal during this phase will not succeed; those sticky records
 * are removed when the operation completes.
 * <br>
 *
 * All permanent failures raise an alarm.
 *
 * Created by arossi on 1/22/15.
 */
public final class PnfsUpdateWorker implements Runnable, TaskCompletionHandler {
    private static final Logger LOGGER
                    = LoggerFactory.getLogger(PnfsUpdateWorker.class);

    private static final ImmutableList ONLINE_STICKY_RECORD
                    = ImmutableList.of(new StickyRecord("system",
                                                        StickyRecord.NON_EXPIRING));

    private static final String ABORT_MESSAGE
                    = "Failed to replicate ({}, source {}) during phase {}; "
                    + "source pools tried: {}; "
                    + "exception {}, cause: {}. "
                    + "Replication cannot proceed at this time; a best effort "
                    + "at retry will be made during the next periodic watchdog "
                    + "scan.";

    private static final String FAILED_UNPIN_MESSAGE
                    = "Failed to unpin ({}, pool {}); "
                    + "exception {}, cause: {}. "
                    + "The sticky record belonging to the replica manager "
                    + "should be removed manually using the admin command.";

    enum State {
        START,
        POOLGROUPINFO,      // cache access, possibly calls pool manager
        FILEINFO,           // cache access, possibly calls chimera
        CACHEENTRYINFO,     // sends message to source pool
        MIGRATION,          // executes migration task
        REDUCTION,          // executes potential reduction/cleanup
        DONE
    }

    private final PnfsId pnfsId;
    private final ReplicaManagerHub hub;

    /*
     * Keeps track of potential source pools for the copy task.
     * In the case of migration failure, a new pool might be selected
     * as source after excluding already tried pools.
     */
    private final Set<String> triedSourcePools = new HashSet<>();

    /*
     * If the initial source pool has migration issues, we
     * may be able to select a different source, so we do not
     * make this field final.
     */
    private String pool;

    private PoolGroupInfo poolGroupInfo;
    private FileAttributes attributes;

    /*
     * This represents the minimal set of locations which the migration task
     * has deemed or promoted to replica status, and which satisfy
     * the replication count requirement.
     */
    private Collection<String> confirmed;

    private State state = State.START;

    public PnfsUpdateWorker(String pool,
                            PnfsId pnfsId,
                            ReplicaManagerHub hub) {
        this.pool = pool;
        this.pnfsId = pnfsId;
        this.hub = hub;
    }

    @Override
    public void run() {
        /*
         * NOTE that the MIGRATION logic is different in that the next
         * phase is set by the completion handler.  This is because the
         * migration task queues itself onto a separate scheduled executor.
         */
        switch (state) {
            case START:                                 nextState(); break;
            case POOLGROUPINFO:  getPoolGroupInfo();    nextState(); break;
            case FILEINFO:       getFileInfo();         nextState(); break;
            case CACHEENTRYINFO: getCacheEntryInfo();   nextState(); break;
            case MIGRATION:      doMigration();         break;
            case REDUCTION:      doReduction();         nextState(); break;
            default:                                    break;
        }
    }

    @Override
    public void taskCancelled(Task task) {
        failed(new Exception(
                        String.format("Migration task %s on %s was cancelled.",
                                        task.getId(), pool)));
    }

    @Override
    public void taskFailed(Task task, int rc, String msg) {
        try {
            LOGGER.debug("Migration task {} for {} failed; looking for another"
                            + " source pool", task.getId(), pnfsId);

            /*
             * Refresh the file attributes.
             */
            attributes = hub.getPnfsInfoCache().getAttributes(pnfsId);
            Collection<String> locations = attributes.getLocations();
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
                taskFailedPermanently(task, rc, msg);
            } else {
                /*
                 * We don't need a balancing selection strategy here,
                 * as we are choosing another source from which
                 * to replicate, not an optimal target pool
                 * (even if the source pool is "hot", this is a one-time
                 * read).  We just choose randomly from the remaining pools.
                 */
                pool = hub.randomSelector.select(locations);

                /*
                 * Since we have chosen from the pool group, we can
                 * reset to that state and proceed to get file attributes
                 * and cache entry info for the new source.
                 */
                state = State.POOLGROUPINFO;
                nextState();
            }
        } catch (ExecutionException t) {
            taskFailedPermanently(task, rc, msg);
        }
    }

    @Override
    public void taskFailedPermanently(Task task, int rc, String msg) {
        failed(new Exception(String.format("rc=%s; %s.", rc, msg)));
        done();
    }

    @Override
    public void taskCompleted(Task task) {
        /*
         * TODO we need this from the Migration Task
         */
        // confirmed = task.getConfirmedLocations();
        if (confirmed == null || confirmed.isEmpty() ) {
            failed(new Exception("Migration task returned no confirmed locations."));
            return;
        }

        nextState();
    }

    private void doMigration() {
        TaskParameters taskParameters
                        = ReplicationTaskParametersFactory.create(pool,
                        pnfsId,
                        attributes,
                        hub);

        triedSourcePools.add(pool);

        /*
         * Calling run() causes the task to execute via the provided executor.
         * The TaskCompletionHandler methods are invoked upon termination.
         */
        new Task(taskParameters,
                        this,
                        pool,
                        pnfsId,
                        EntryState.CACHED,
                        ONLINE_STICKY_RECORD,
                        Collections.EMPTY_LIST,
                        attributes).run();
    }

    private void done() {
        state = State.DONE;
        LOGGER.debug("completed processing of {} on {}.", pnfsId, pool);
    }


    /*
     * The methods pin and remove act like barriers, so
     * this method call is in effect synchronous.
     */
    private void doReduction() {
        List<PnfsId> pnfsIds = new ArrayList<>();
        pnfsIds.add(pnfsId);
        CellStubFactory factory = hub.getPoolStubFactory();

        try {
            List<String> allLocations = hub.getPnfsInfoCache()
                                           .getAllLocationsFor(pnfsId);
            pin(pnfsIds, allLocations, true, factory);
            remove(pnfsIds, allLocations, factory);
        } catch (CacheException | InterruptedException | ExecutionException e) {
            /*
             * No alarm is necessary here, since only the reduction phase
             * has failed.
             */
            LOGGER.error("A problem occurred during {} for {}: {}, cause {}. "
                          + "This means that unnecessary copies may still exist; "
                          + "A best effort at removal will be made during "
                          + "the next periodic watchdog "
                          + "scan.",
                            state,
                            pnfsId,
                            e.getMessage(),
                            String.valueOf(e.getCause()));
        } finally {
            try {
                pin(pnfsIds, confirmed, false, factory);
            } catch (InterruptedException | ExecutionException e) {
                /*
                 *  This should generate an alarm, because we don't want
                 *  to leave sticky records blocking all removal.
                 */
                LOGGER.error(AlarmMarkerFactory.getMarker(PredefinedAlarm.FAILED_REPLICATION,
                                                pnfsId.toString()),
                                FAILED_UNPIN_MESSAGE,
                                pnfsId,
                                pool,
                                e.getMessage(),
                                String.valueOf(e.getCause()));
            }
        }
    }

    /*
     * Sends an alarm.
     */
    private void failed(Exception e) {
        LOGGER.error(AlarmMarkerFactory.getMarker(PredefinedAlarm.FAILED_REPLICATION,
                                                  pnfsId.toString()),
                        ABORT_MESSAGE,
                        pnfsId,
                        pool,
                        state,
                        triedSourcePools,
                        e == null ? "" : e.getMessage(),
                        e == null ? "" : String.valueOf(e.getCause()));
        done();
    }

    private void getCacheEntryInfo() {
        CacheEntryInfoMessage message = new CacheEntryInfoMessage(pnfsId);
        Future<CacheEntryInfoMessage> future
                        = hub.getPoolStubFactory().getCellStub(pool)
                        .send(message);

        LOGGER.trace("Sent CacheEntryInfoMessage for {} to {}.", pnfsId, pool);

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
             * file was for some reason not ONLINE. In this case, the
             * info object is <code>null</code>.
             */
            LOGGER.warn("Attempt to get cache entry was cancelled: {}.", message);
            done();
        }
    }

    private void getFileInfo() {
        try {
            attributes = hub.getPnfsInfoCache().getAttributes(pnfsId);
            if (!attributes.getAccessLatency().equals(AccessLatency.ONLINE)) {
                LOGGER.debug("AccessLatency of {} is not ONLINE; ignoring ...",
                                pnfsId);
                done();
            }
        } catch (ExecutionException t) {
            failed(t);
        }
    }

    private void getPoolGroupInfo() {
        try {
            poolGroupInfo = hub.getPoolInfoCache().getPoolGroupInfo(pool);
            if (!poolGroupInfo.isResilient()) {
                LOGGER.debug("{} does not belong to a resilient group", pool);
                done();
            }
        } catch (ExecutionException t) {
            failed(t);
        }
    }

    private void launch() {
        LOGGER.debug("Launching phase {} for {} on {}.", state, pnfsId, pool);

        switch (state) {
            case POOLGROUPINFO:
                hub.getPoolGroupInfoTaskExecutor().execute(this);
                break;
            case FILEINFO:
                hub.getPnfsInfoTaskExecutor().execute(this);
                break;
            case CACHEENTRYINFO:
                hub.getPnfsInfoTaskExecutor().execute(this);
                break;
            case MIGRATION:
                /*
                 * We do the preparation for task execution on
                 * the current thread (pnfsInfoTaskExecutor).
                 */
                run();
                break;
            case REDUCTION:
                hub.getReductionTaskExecutor().execute(this);
                break;
            case DONE:
                /*
                 * Should not get here ...
                 */
                return;
            default:
                String message = String.format("Worker for %s on %s launched in "
                                                + "an illegal state %s.",
                                pnfsId, pool, state);
                throw new IllegalStateException(message);
        }
    }

    private void nextState() {
        LOGGER.debug("completed phase {} for {} on {}.", state, pnfsId, pool);

        switch (state) {
            case START:             state = State.POOLGROUPINFO;    launch(); break;
            case POOLGROUPINFO:     state = State.FILEINFO;         launch(); break;
            case FILEINFO:          state = State.CACHEENTRYINFO;   launch(); break;
            case CACHEENTRYINFO:    state = State.MIGRATION;        launch(); break;
            case MIGRATION:         state = State.REDUCTION;        launch(); break;
            case REDUCTION:         done();                         break;
            default:                                                break;
        }
    }

    private void pin(Collection<PnfsId> pnfsIds,
                     Collection<String> locations,
                     boolean set,
                     CellStubFactory factory)
                    throws ExecutionException, InterruptedException {
        Collection<Future<StickyReplicasMessage>> toJoin = new ArrayList<>();
        StickyReplicasMessage msg = null;

        for (String location: locations) {
            msg = new StickyReplicasMessage(location, pnfsIds, set);
            toJoin.add(factory.getCellStub(location).send(msg));
        }

        for (Future<StickyReplicasMessage> future: toJoin) {
            /*
             * Should block until ready.
             */
            msg = future.get();
            if (msg.iterator().hasNext()) {
                Exception e = new Exception("Was unable to set replica manager "
                                + "sticky record for " + pnfsId + " to " + set
                                + " on pool " + msg.pool);
                throw new ExecutionException(e);
            }
        }
    }

    private void remove(Collection<PnfsId> pnfsIds,
                    Collection<String> locations, CellStubFactory factory)
                    throws ExecutionException, InterruptedException {
        Collection<Future<RemoveReplicasMessage>> toJoin = new ArrayList<>();
        RemoveReplicasMessage msg = null;

        for (String location: locations) {
            if (!confirmed.contains(location)) {
                msg = new RemoveReplicasMessage(location,pnfsIds);
                toJoin.add(factory.getCellStub(location).send(msg));
            }
        }

        for (Future<RemoveReplicasMessage> future: toJoin) {
            /*
             * Should block until ready.
             */
            msg = future.get();
            if (msg.iterator().hasNext()) {
                /*
                 * Does not require an alarm.  Cleanup will be attempted
                 * again at the next periodic watchdog scan.
                 */
                LOGGER.warn("Was unable to remove replica for {} on pool {}.",
                                pnfsId, msg.pool);
            }
        }
    }
}
