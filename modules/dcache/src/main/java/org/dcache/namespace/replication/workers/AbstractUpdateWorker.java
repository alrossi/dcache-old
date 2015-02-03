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
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import org.dcache.alarms.AlarmMarkerFactory;
import org.dcache.alarms.PredefinedAlarm;
import org.dcache.namespace.replication.ReplicaManagerHub;
import org.dcache.namespace.replication.caches.PnfsInfoCache;
import org.dcache.namespace.replication.data.PnfsIdInfo;
import org.dcache.namespace.replication.data.PoolGroupInfo;
import org.dcache.util.replication.CellStubFactory;
import org.dcache.util.replication.CollectionElementSelectionStrategy;
import org.dcache.vehicles.replication.RemoveReplicasMessage;
import org.dcache.vehicles.replication.StickyReplicasMessage;

/**
 * Encapsulates fields and functions common to both pnfsid and pool status
 * workers.
 * <p/>
 * This includes providing for the pinning and removal of replicas
 * grouped by location/pool, selection of copies to remove,
 * and the delivery of a future useful for joining on the
 * worker until it completes.
 *
 * Created by arossi on 1/27/15.
 */
public abstract class AbstractUpdateWorker<F> implements Runnable {
    protected static final Logger LOGGER
                    = LoggerFactory.getLogger(AbstractUpdateWorker.class);

    protected static final CollectionElementSelectionStrategy<String> randomSelector
                    = new CollectionElementSelectionStrategy<String>() {

        private Random random = new Random(System.currentTimeMillis());

        public String select(Collection<String> collection) {
            if (collection.isEmpty()) {
                return null;
            }

            int index = Math.abs(random.nextInt()) % collection.size();

            Iterator<String> it = collection.iterator();

            for (int i = 0; i < index - 1; i++) {
                it.next();
            }

            return it.next();
        }
    };

    protected static final String FAILED_REDUCTION_MESSAGE
                    = "{}. \n"
                    + "This means that unnecessary copies may still exist; "
                    + "A best effort at removal will be made during "
                    + "the next periodic watchdog scan.";

    private static final String FAILED_UNPIN_MESSAGE
                    = "{}. \n"
                    + "The sticky records belonging to the replica manager "
                    + "may be removed manually using the admin command; "
                    + "if left, they will not expire for (at most) 12 hours.";

    private static final String FORCED_CANCELLATION_WARNING
                    = "User has cancelled this replication task.";

    public final UUID workerId = UUID.randomUUID();

    protected final ReplicaManagerHub hub;
    protected final CellStubFactory stubFactory;

    /**
     * Binning for locations containing replicas which must be unpinned
     * after a removal operation completes.
     */
    protected Multimap<String, PnfsId> toUnpin;

    /**
     * Binning for locations containing replicas which must be removed.
     */
    protected Multimap<String, PnfsId> toRemove;

    protected String poolName;
    protected PoolGroupInfo poolGroupInfo;

    /**
     *  If the worker has submitted itself to an executor queue for
     *  a particular phase, this future is associated with that phase.
     */
    protected Future running;
    protected boolean cancelled = false;

    protected AbstractUpdateWorker(String poolName, ReplicaManagerHub hub) {
        /*
         * If the initial source pool has migration issues, a child class may
         * be able to select a different source; hence, the parent
         * field is not final.
         */
        this.poolName = poolName;
        this.hub = hub;
        this.stubFactory = hub.getCellStubFactory();
    }

    /**
     * Used for joining on the worker until it reaches a done/failed state.
     * e.g., PoolStatusUpdateWorker joins on PnfsUpdateWorkers;
     *       ResilienceWatchdog joins on PoolScanWorkers.
     *
     * @return a future which will return the typed value when the worker
     *         has completed.
     */
    public Future<F> getWorkerFuture() {
        return new Future<F>() {
            final AbstractUpdateWorker<F> stateful = AbstractUpdateWorker.this;

            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                stateful.cancel();
                return cancelled;
            }

            @Override
            public boolean isCancelled() {
                return cancelled;
            }

            @Override
            public boolean isDone() {
                synchronized(stateful) {
                    return stateful.isDone();
                }
            }

            @Override
            public F get() throws InterruptedException, ExecutionException {
                synchronized (stateful) {
                    while (!stateful.isDone()) {
                        /*
                         * It is imperative that the worker implementation
                         * call notifyAll on itself when its task completes,
                         * so as to unblock the waiting thread here.
                         */
                        stateful.wait();
                    }
                }
                return returnValue();
            }

            @Override
            public F get(long timeout, TimeUnit unit)
                            throws InterruptedException, ExecutionException,
                            TimeoutException {
                throw new UnsupportedOperationException("Use get().");
            }
        };
    }

    /**
     *  @return name of the pool targeted by this worker.
     */
    public String getPoolName() {
        return poolName;
    }

    /*
     *  Used internally in registry.
     */
    public String getWorkerName() {
        return poolName;
    }

    /**
     * Creates an inverted map of location to pnfsIds by accessing the
     * PnfsInfoCache.
     *
     * @param pnfsIds to map to their locations
     * @return inverse Multimap of location to pnfsId list.
     * @throws ExecutionException
     * @throws CacheException
     */
    protected Multimap<String, PnfsId> binByLocation(Collection<PnfsId> pnfsIds)
                    throws ExecutionException, CacheException {
        PnfsInfoCache cache = hub.getPnfsInfoCache();
        Multimap<String, PnfsId> binned = ArrayListMultimap.create();
        for (PnfsId pnfsId : pnfsIds) {
            PnfsIdInfo info = cache.getPnfsIdInfo(pnfsId);
            Collection<String> locations = info.refreshLocations();
            for (String location : locations) {
                binned.put(location, pnfsId);
            }
        }
        return binned;
    }

    /**
     * Checks for running and cancels it.
     * Also checks for leftover removes/unpins.
     * Should be called through the Future on the worker.
     */
    protected void cancel() {
        synchronized(this) {
            failed(new Exception(FORCED_CANCELLATION_WARNING));
            if (running != null) {
                running.cancel(true);
            }
            clearPinned();
        }
    }

    /*
    * Retrieves the pool group information from the cache, which uses
    * a refreshing embedded pool monitor to load. If the group is not
    * resilient, sets the state to DONE.
    */
    protected void getPoolGroupInfo() {
        try {
            poolGroupInfo = hub.getPoolInfoCache().getPoolGroupInfo(poolName);
            if (!poolGroupInfo.isResilient()) {
                LOGGER.debug("{} does not belong to a resilient group",
                                poolName);
                done();
            }
        } catch (ExecutionException t) {
            failed(t);
        }
    }

    /**
     * Adds a sticky record with replica manager as owner on the pnfsids
     * for each location. This is done via a message
     * sent to the replica manager handler on the pool itself.
     * <p/>
     * Even though the method may be called in order to pin all replicas
     * of a given pnfsid, it is implemented by location for efficiency.
     * <p/>
     * The future returned from the send call is stored and
     * get() is then called on each, creating a barrier.
     * <p/>
     * Pins which fail are reported on a pnfsId-basis without regard
     * for the location, as a missing pin anywhere will invalidate
     * the protection for a subsequent operation on that pnfsId (such
     * as remove).
     *
     */
    protected Collection<PnfsId> pin(Map<String, Collection<PnfsId>> map) {
        Collection<Future<StickyReplicasMessage>> toJoin = new ArrayList<>();
        StickyReplicasMessage msg;

        for (String location: map.keySet()) {
            msg = new StickyReplicasMessage(location, map.get(location), true);
            LOGGER.trace("Sending StickyReplicasMessage {}.", msg);
            toJoin.add(stubFactory.getPoolStub(location).send(msg));
        }

        Collection<PnfsId> failed = new HashSet<>();

        for (Future<StickyReplicasMessage> future: toJoin) {
            try {
                msg = future.get();
            } catch (InterruptedException | ExecutionException e) {
                /*
                 *  This is here because of the API, but in the
                 *  case of these calls, the exceptions have been
                 *  suppressed.  Failure is denoted by a non-empty
                 *  collection, as follows. If an exception is thrown
                 *  here, there is definitely a bug in the code.
                 */
                throw new RuntimeException("An unexpected exception was thrown "
                                + "during a pin operation.", e);
            }

            LOGGER.trace("Returned StickyReplicasMessage {}.", msg);

            Iterator<PnfsId> it = msg.iterator();
            if (it.hasNext()) {
                do {
                    failed.add(it.next());
                } while (it.hasNext());
            }
        }

        return failed;
    }

    /*
     * Removes from the given location the cache entries of the pnfsids
     * provided.  This is done via a message sent to the replica manager
     * handler on the pool itself.
     * <p/>
     * Even though the method is called in order to remove excess replicas
     * of a given pnfsid, it is implemented by location for efficiency.
     * <p/>
     * The future returned from the send call is stored and
     * get() is then called on each, creating a barrier.
     * <p/>
     * An attempt to rollback pins is made on failed calls.
     * Note that any attempt to pin or unpin a missing pnfsid on a pool
     * will fail silently, so the calls to pin or unpin an entry
     * which no longer exists will not cause the operation as a whole to fail.
     *
     * @param map of (location, [pnfsIds]) to remove.
     * <p/>
     */
    protected void remove() {
        Map<String, Collection<PnfsId>> map = toRemove.asMap();
        Collection<Future<RemoveReplicasMessage>> toJoin = new ArrayList<>();
        RemoveReplicasMessage msg;

        for (String location : map.keySet()) {
            msg = new RemoveReplicasMessage(location, map.get(location));
            LOGGER.trace("Sending RemoveReplicasMessage {}.", msg);
            toJoin.add(stubFactory.getPoolStub(location).send(msg));
        }

        for (Future<RemoveReplicasMessage> future : toJoin) {
            try {
                msg = future.get();
            } catch (InterruptedException | ExecutionException e) {
               /*
                *  This is here because of the API, but in the
                *  case of these calls, the exceptions have been
                *  suppressed.  Failure is denoted by a non-empty
                *  collection, as follows. If an exception is thrown
                *  here, there is definitely a bug in the code.
                */
                throw new RuntimeException("An unexpected exception was "
                                + "thrown during a remove operation.", e);
            }

            LOGGER.trace("Returned RemoveReplicasMessage {}.", msg);

            Iterator<PnfsId> it = msg.iterator();

            if (!it.hasNext()) {
                map.remove(msg.pool);
            } else {
                Collection<PnfsId> pnfsids = map.get(msg.pool);
                pnfsids.clear();
                while (msg.iterator().hasNext()) {
                    pnfsids.add(it.next());
                }
            }
        }

        if (!map.isEmpty()) {
            StringBuilder details = new StringBuilder("Was unable to remove the "
                                                     + "following replicas.\n");
            for (String location: map.keySet()) {
                details.append("\t").append(location).append(":\n");
                Collection<PnfsId> pnfsIds = map.get(location);
                for (PnfsId pnfsId: pnfsIds) {
                    details.append("\t\t").append(pnfsId).append("\n");
                }
                details.append("\n");
            }

           /*
            * No alarm is necessary here,
            * since only the reduction phase has failed.
            */
            LOGGER.error(FAILED_REDUCTION_MESSAGE, details);

           /*
            * But we need to try to unpin the failed removes.
            */
            unpin(map);
        }
    }

    /*
     * All locations are pinned in advance of doing the
     * remove computations;  binByLocation automatically refreshes
     * the locations on the pnfsIds.
     *
     * The logic for this is as follows:  By refreshing here, any
     * location removed after that call but before we pin the files will
     * result in a failed pin. The pnfsid for that pin can then
     * be excluded from attempted removes. Note that any location
     * newly created while this worker is running is not handled here,
     * as it would spawn a second PnfsUpdate anyway.
     */
    protected void selectForReduction(Collection<PnfsId> pnfsIds,
                                      Multimap<String, PnfsId> toRemove,
                                      Collection<String> toExclude) {
        PnfsInfoCache cache = hub.getPnfsInfoCache();

        try {
            toUnpin = binByLocation(pnfsIds);

            Collection<PnfsId> failed = pin(toUnpin.asMap());

            /*
             *  Remove all the failed pins.  We can't remove replicas
             *  for these.
             */
            for (PnfsId invalid: failed) {
                pnfsIds.remove(invalid);
                for (Iterator<Entry<String, PnfsId>> it
                                     = toUnpin.entries().iterator();
                     it.hasNext();) {
                    if (it.next().getValue().equals(invalid)) {
                        it.remove();
                    }
                }
            }

            Collection<String> active
                            = hub.getPoolInfoCache().findAllActivePools();

            for (Iterator<PnfsId> it = pnfsIds.iterator(); it.hasNext();) {
                PnfsId pnfsId = it.next();
                PnfsIdInfo info = cache.getPnfsIdInfo(pnfsId);

                /*
                 * Keep removing locations from the info location list
                 * for the given pnfsId, adding entries to the map to remove
                 * until we reach the limit. For each selected source,
                 * remove the pnfsId from the list which has been pinned
                 * (toUnpin). This should partition all location lists
                 * between unpin and remove.
                 */
                int count = info.getNumberNeeded(active);
                if (count < 0) {
                    /*
                     * Inactive pools have already been removed.
                     * Now remove any other pools to exclude.
                     */
                    Collection<String> pools = info.getLocations();
                    for (String exclude: toExclude) {
                        pools.remove(exclude);
                    }

                    int limit = Math.abs(count);
                    for (int i = 0; i < limit; i++) {
                        String source = randomSelector.select(pools);
                        if (source == null) {
                            /*
                             * pools was empty.  No alarm is necessary,
                             * since this is for removal.
                             */
                            break;
                        }
                        toRemove.put(source, pnfsId);
                        pools.remove(source);
                        toUnpin.remove(source, pnfsId);
                    }
                }

                /*
                 * Unlike with migration, the entry is no longer needed
                 * for reduction.
                 */
                cache.invalidate(pnfsId);
                it.remove();
            }

            LOGGER.debug("selectForReduction completed for PoolUpdateWorker on {}.",
                            poolName);
            LOGGER.trace("selectForReduction, toRemove: {}.", toRemove);
        } catch (CacheException | ExecutionException | InterruptedException e) {
            /*
             * We need to attempt to unpin first.
             */
            clearPinned();
            failed(e);
        }
    }

    /**
     * Removes a sticky record with replica manager as owner on the pnfsids
     * for each location. This is done via a message
     * sent to the replica manager handler on the pool itself.
     * <p/>
     * Even though the method may be called in order to pin all replicas
     * of a given pnfsid, it is implemented by location for efficiency.
     * <p/>
     * The future returned from the send call is stored and
     * get() is then called on each, creating a barrier.
     * <p/>>
     * Failed unpins provoke an alarm. Note that, in any case,
     * the Replica Manager sticky record has an expiration of 12 hours.
     *
     * @param map of (location, [pnfsIds]) to unpin.
     */
    protected void unpin(Map<String, Collection<PnfsId>> map) {
        Collection<Future<StickyReplicasMessage>> toJoin = new ArrayList<>();
        StickyReplicasMessage msg;

        for (String location : map.keySet()) {
            msg = new StickyReplicasMessage(location, map.get(location), false);
            LOGGER.trace("Sending StickyReplicasMessage {}.", msg);
            toJoin.add(stubFactory.getPoolStub(location).send(msg));
        }

        for (Future<StickyReplicasMessage> future : toJoin) {
            try {
                msg = future.get();
            } catch (InterruptedException | ExecutionException e) {
                /*
                 *  This is here because of the API, but in the
                 *  case of these calls, the exceptions have been
                 *  suppressed.  Failure is denoted by a non-empty
                 *  collection, as follows. If an exception is thrown
                 *  here, there is definitely a bug in the code.
                 */
                throw new RuntimeException("An unexpected exception was thrown "
                                + "during an unpin operation.", e);
            }

            LOGGER.trace("Received StickyReplicasMessage {}.", msg);

            Iterator<PnfsId> it = msg.iterator();

            if (!it.hasNext()) {
                map.remove(msg.pool);
            } else {
                Collection<PnfsId> pnfsids = map.get(msg.pool);
                pnfsids.clear();
                while (msg.iterator().hasNext()) {
                    pnfsids.add(it.next());
                }
            }
        }

        if (!map.isEmpty()) {
            StringBuilder details = new StringBuilder("Was unable to unpin the "
                                                      + "following replicas.\n");
            for (String location: map.keySet()) {
                details.append("\t").append(location).append(":\n");
                Collection<PnfsId> pnfsIds = map.get(location);
                for (PnfsId pnfsId: pnfsIds) {
                    details.append("\t\t").append(pnfsId).append("\n");
                }
                details.append("\n");
            }

           /*
            *  This should generate an alarm, because we don't want
            *  to leave sticky records blocking all removal.
            */
            LOGGER.error(AlarmMarkerFactory.getMarker(PredefinedAlarm.FAILED_REPLICATION,
                                                      poolName),
                            FAILED_UNPIN_MESSAGE,
                            details);
        }
    }

    protected void unregister() {
        hub.getRegistry().unregister(this);
    }

    /*
     * Shared state-based logic.
     */

    protected abstract void done();

    protected abstract void failed(Exception e);

    protected abstract boolean isDone();

    protected abstract void launch();

    protected abstract void nextState();

    protected abstract void reduce();

    protected abstract void register();

    protected abstract void replicate();

    protected abstract F returnValue();

    private void clearPinned() {
        if (toRemove != null && !toRemove.isEmpty()) {
            unpin(toRemove.asMap());
        }

        if (toUnpin != null && !toUnpin.isEmpty()) {
            unpin(toUnpin.asMap());
        }
    }
}