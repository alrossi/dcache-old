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

import com.google.common.collect.Multimap;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import org.dcache.alarms.AlarmMarkerFactory;
import org.dcache.alarms.PredefinedAlarm;
import org.dcache.namespace.replication.PoolStatusNotifier;
import org.dcache.namespace.replication.ReplicaManagerHub;
import org.dcache.namespace.replication.caches.PnfsInfoCache;
import org.dcache.namespace.replication.data.PnfsIdInfo;

/**
 * Encapsulates fields and functions common to status message processing and
 * full scanning for a given pool.
 * <p/>
 * This includes preloading of the cached pnfsid info, selection of
 * pools and pnfsids for copying, the actual processing of replication
 * and reduction tasks, waiting for the completion of workers,
 * and callback/notification to the notifier and any waiting threads.
 *
 * Created by arossi on 1/30/15.
 */
public abstract class PoolUpdateWorker extends AbstractUpdateWorker<String> {
    protected static final String ABORT_MESSAGE
                    = "Failed to handle {} for {} during phase {}; "
                    + "exception {}, cause: {}. "
                    + "Operation cannot proceed at this time; a best effort "
                    + "at retry will be made during the next periodic watchdog "
                    + "scan.";

    enum BoundCheck {
        UPPER_MIN, LOWER_MAX
    }

    /**
     * All pnfsIds at this pool location.
     */
    protected final Collection<PnfsId> pnfsIds = new HashSet<>();

    /**
     * For joining on PnfsUpdateWorkers.
     */
    protected final Collection<Future<PnfsId>> workerFutures = new ArrayList<>();

    /**
     * For handling/blocking status messages received while worker is
     * waiting or running.
     */
    protected PoolStatusNotifier notifier;

    protected PoolUpdateWorker(String poolName, ReplicaManagerHub hub) {
        super(poolName, hub);
    }

    @Override
    protected void cancel() {
        /*
         * Abstract method is synchronized,
         * but we don't hold the lock after it completes
         */
        super.cancel();

        synchronized(workerFutures) {
            if (workerFutures != null) {
                for (Iterator<Future<PnfsId>> it
                                     =  workerFutures.iterator(); it.hasNext();) {
                    it.next().cancel(true);
                }
            }
        }
    }

    @Override
    protected void done() {
        setDone();

        if (notifier != null) {
            notifier.taskCompleted();
        }

        /*
         * For the sake of any caller holding a future against this worker.
         */
        synchronized (this) {
            notifyAll();
        }

        LOGGER.debug("PoolUpdateWorker for {} done, "
                                        + "notified notifier and future.",
                        poolName);

        unregister();
    }

    /*
     *  Remove by pool, then unpin the rest (by pool).
     */
    protected void doReduction() {
        LOGGER.debug("PoolUpdateWorker for {}, calling remove.", poolName);
        remove();
        LOGGER.debug("PoolUpdateWorker for {}, calling unpin.", poolName);
        unpin(toUnpin.asMap());
    }

    /*
     * Currently the underlying Migration Task supports only
     * one pnfsId; this is reflected in the PnfsUpdateWorker
     * as well.  I am not sure if batching is feasible, but
     * the map has been constructed as if it were just in case.
     */
    protected synchronized void doReplication(Multimap<String, PnfsId> toCopy) {
        for (String location: toCopy.keySet()) {
            Collection<PnfsId> pnfsIds = toCopy.get(location);
            for (PnfsId pnfsId: pnfsIds) {
                /*
                 * Note that the information for each pnfsId should
                 * probably remain in the cache for long enough
                 * that the worker merely retrieve it from there
                 * rather than initiating a new load if the queue
                 * is short or moderately sized.
                 */
                PnfsUpdateWorker worker
                                = new PnfsUpdateWorker(location, pnfsId, hub);
                /*
                 * When run() is called in the START state, the worker
                 * immediately queues itself onto the first executor queue.
                 */
                worker.run();
                workerFutures.add(worker.getWorkerFuture());

                LOGGER.debug("Started PnfsUpdateWorker for {} on {}.",
                                pnfsId, location);
            }
        }
    }

    /*
     * Request pnfsids and counts from namespace according
     * to boundary constraints. Then iterate over each, caching
     * the info, and checking storage group constraints against actual
     * counts, and discarding if the current count meets them.
     * Otherwise, the pnfsId is added to the list of pnfsIds.
     */
    protected void loadPnfsIdInfo(BoundCheck type, Set<String> toExclude)
                    throws CacheException, ParseException {
        String filter;
        int bound;

        switch(type) {
            case UPPER_MIN:
                bound = poolGroupInfo.getUpperBoundForMin();
                filter = " < " + bound;
                break;
            case LOWER_MAX:
                bound = poolGroupInfo.getLowerBoundForMax();
                filter = " > " + bound;
                break;
            default:
                bound = 1;
                filter = " > " + bound;
                break;
        }

        Map<String, Integer> counts
                        = hub.getAccess().getPnfsidCountsFor(poolName, filter);
        PnfsInfoCache cache = hub.getPnfsInfoCache();

        pnfsIds.clear();

        try {
            for (Iterator<String> it = counts.keySet().iterator(); it.hasNext();) {
                String key = it.next();
                PnfsId pnfsId = new PnfsId(key);
                PnfsIdInfo info = cache.getPnfsIdInfo(pnfsId);
                if (info != null) {
                    info.setConstraints(poolGroupInfo);
                }


                switch(type) {
                    case UPPER_MIN:
                        if (counts.get(key) >= info.getMinimum()) {
                            cache.invalidate(pnfsId);
                            it.remove();
                            continue;
                        }
                        break;
                    case LOWER_MAX:
                        if (counts.get(key) <= info.getMaximum()) {
                            cache.invalidate(pnfsId);
                            it.remove();
                            continue;
                        }
                        break;
                    default:
                        break;
                }

                pnfsIds.add(pnfsId);
                it.remove();
            }
        } catch (ExecutionException e) {
            /*
             *  Fail fast.
             */
            failed(e);
        }
    }

    @Override
    protected String returnValue() {
        return poolName;
    }

    /*
     *  Examines all active locations against requirements, with or
     *  without the current pool included (this is usually excluded during
     *  a DOWN update, but not during a scan).
     */
    protected void selectForReplication(Collection<PnfsId> pnfsIds,
                                        Multimap<String, PnfsId> toCopy,
                                        boolean excludeCurrentPool) {
        PnfsInfoCache cache = hub.getPnfsInfoCache();

        try {
            Collection<String> active = hub.getPoolInfoCache().findAllActivePools();

            /*
             * This is usually when the current pool is DOWN.  It should
             * already not be listed there, but this is just in case.
             */
            if (excludeCurrentPool) {
                active.remove(poolName);
            }

            for (Iterator<PnfsId> it = pnfsIds.iterator(); it.hasNext();) {
                PnfsId pnfsId = it.next();
                PnfsIdInfo info = cache.getPnfsIdInfo(pnfsId);
                int needed = info.getNumberNeeded(active);
                if (needed > 0) {
                    /*
                     * Inactive pools have already been removed. Since
                     * we have ensured that the current pool is treated
                     * as inactive if excludeCurrentPool is true, no
                     * other adjustment is needed here.
                     */
                    Collection<String> pools = info.getLocations();
                    String source = randomSelector.select(pools);
                    if (source == null) {
                        /*
                         * No other pools to choose.
                         */
                        LOGGER.error(AlarmMarkerFactory.getMarker(PredefinedAlarm.FAILED_REPLICATION,
                                                                  pnfsId.toString()),
                                                        "No active pools available for "
                                                        + "migration; number of replicas "
                                                        + "needed: {}.", needed);
                    } else {
                        toCopy.put(source, pnfsId);
                    }
                }
                it.remove();
            }

            LOGGER.debug("selectForReplication completed for PoolUpdateWorker on {}.",
                            poolName);
            LOGGER.trace("selectForReplication, toCopy: {}.", toCopy);
        } catch (ExecutionException | InterruptedException | CacheException e) {
            failed(e);
        }
    }

    /*
     *  Barrier on launched PnfsUpdateWorkers.
     */
    protected void waitForCompletion() {
        LOGGER.debug("PoolUpdateWorker on {} waiting for "
                                        + "migration tasks to complete.",
                        poolName);

        StringBuilder error
                        = new StringBuilder("Not all pnfs update workers completed:\n%s.");
        synchronized(workerFutures) {
            for (Iterator<Future<PnfsId>> it = workerFutures.iterator(); it.hasNext(); ) {
                Future<PnfsId> future = it.next();
                PnfsId pnfsId = null;
                try {
                    pnfsId = future.get();
                } catch (InterruptedException | ExecutionException e) {
                    error.append(e.getMessage()).append(", cause: ").append(
                                    e.getCause()).append("\n");
                }

                if (pnfsId != null) {
                    it.remove();
               /*
                * Note that the single worker will already have invalidated the
                * cache entry for this pnfsid.
                */
                }
            }

            if (workerFutures.isEmpty()) {
                done();
            } else {
                failed(new Exception(String.format(error.toString(), pnfsIds)));
            }
        }
    }

    /**
     * A callback from the notifier to have itself removed from the cache.
     */
    void unregisterNotifier() {
        LOGGER.debug("Notifier called back to PoolUpdateWorker for {} "
                        + "to unregister.", poolName);
        hub.getPoolStatusCache().unregisterPoolNotifier(notifier);
    }

    protected abstract void startNotifier();

    protected abstract void setDone();
}
