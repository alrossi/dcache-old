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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;

import diskCacheV111.util.PnfsId;
import diskCacheV111.util.RetentionPolicy;
import org.dcache.alarms.AlarmMarkerFactory;
import org.dcache.alarms.PredefinedAlarm;
import org.dcache.namespace.replication.ReplicaManagerHub;
import org.dcache.namespace.replication.caches.PnfsInfoCache;
import org.dcache.namespace.replication.data.PnfsIdInfo;
import org.dcache.namespace.replication.data.PoolStatusMessageType;

/**
 * A worker responsible for all phases of the handling of pool status changes
 * which may require either migration or replica reduction.
 * <p/>
 * Queues itself to run on the appropriate queues for each phase.
 * <p/>
 * A check is first done to make sure the source pool belongs to a resilient
 * group.  The pool group information is retrieved via a cache based on a periodic
 * refresh of the pool monitor. If resilient, the worker initializes and
 * registers a notifier which waits for a configured interval before
 * calling back to the worker to start.  If other status messages for this
 * pool are received during the intervening period, the notifier takes
 * appropriate action by either ignoring the message or restarting the
 * wait with a new mode/type.  The notifier is also responsible for handling
 * messages arriving while the worker is actually running.  Depending on
 * which messages arrive, the notifier may restart the worker after it has
 * completed the current task.  If there are no other status change messages
 * for this pool, the notifier is unregistered when the worker completes.
 * <p/>
 * The first thing the worker does is to execute a query which returns
 * all locations for all pnfsIds found on this pool.  Depending on whether
 * the message type being handled is DOWN or RESTART, the worker either
 * then proceeds to make extra copies for the files requiring them (in the
 * former case) or remove extra copies (in the latter).  If the pool is DOWN,
 * the worker also sends an alarm listing all the files which have only
 * a single copy on the pool (and which have thus become inaccessible).
 * <p/>
 * The current implementation launches a new
 * {@link org.dcache.namespace.replication.workers.PnfsUpdateWorker}
 * for each pnfsid needing extra replicas, but handles all removes
 * on a single thread.  As is usual, all such removes are preceded by
 * the pinning of all locations for all the pnfsIds involved.
 * <p/>
 *
 * Created by arossi on 1/23/15.
 */
public class PoolStatusUpdateWorker extends PoolUpdateWorker {
    private static final String INACCESSIBLE_FILE_MESSAGE
                    = "Resilient pool {} is DOWN; it contains the only copy of "
                    + "the following files which should have a replica elsewhere. "
                    + "Administrator intervention is required.\n\n{}";

    enum State {
        INITIALIZING,        // check for resilience, register notifier
        WAIT_FOR_START,
        FIND_LOCATIONS,      // call database to get map of {(pnfsid: replicas)}
        HANDLE_INACCESSIBLE, // for singletons on a DOWN pool
        SELECT_FOR_COPY,     // bin the pnfsIds according to chosen sources
        SELECT_FOR_REMOVAL,  // bin the pnfsIds according to copies to remove
        REPLICATION,         // for DOWN, launch a PnfsUpdateWorker for each
        REDUCTION,           // for RESTART, pin all, remove selected, unpin remainder
        WAIT_FOR_COMPLETION, // for DOWN, join on the workers
        DONE                 // notifier decides whether to unregister
    }

    /**
     * Binning for source locations selected for replication or reduction.
     */
    private final Multimap<String, PnfsId> locationMap = ArrayListMultimap.create();

    /**
     * Not necessarily the initial type (it may be changed by the notifier
     * before the worker actually begins to process the pool state change).
     */
    private PoolStatusMessageType type;

    private State state;

    public PoolStatusUpdateWorker(String poolName,
                                  PoolStatusMessageType type,
                                  ReplicaManagerHub hub) {
        super(poolName, hub);
        this.type = type;
    }

    /**
     * @return whether the status change being handled is DOWN or RESTART
     */
    public PoolStatusMessageType getType() {
        return type;
    }

    /**
     * Will initialize and start the notifier if the pool is resilient.
     * If the pool group to which the pool belongs is not resilient,
     * will simply set DONE and exit.
     */
    public void initialize() {
        state = State.INITIALIZING;
        launch();
    }

    @Override
    public void run() {
        switch (state) {
            case INITIALIZING:          getPoolGroupInfo();
                                        startNotifier();        nextState(); break;
            case FIND_LOCATIONS:        loadPnfsIdInfo();       nextState(); break;
            case HANDLE_INACCESSIBLE:   handleInaccessible();   nextState(); break;
            case SELECT_FOR_COPY:       selectForReplication(); nextState(); break;
            case SELECT_FOR_REMOVAL:    selectForReduction();   nextState(); break;
            case REPLICATION:           replicate();            nextState(); break;
            case REDUCTION:             reduce();               nextState(); break;
            case WAIT_FOR_COMPLETION:   waitForCompletion();    nextState(); break;
            default:                                                         break;
        }
    }

    public synchronized void start(PoolStatusMessageType type) {
        if (state != State.WAIT_FOR_START && state != State.DONE) {
            throw new IllegalStateException(String.format("Cannot start worker "
                            + "while it is in the %s state.", state));
        }
        this.type = type;
        register();
        nextState();
    }

    @Override
    protected void failed(Exception e) {
        LOGGER.error(ABORT_MESSAGE, type, poolName, state,
                        e == null ? "" : e.getMessage(),
                        e == null ? "" : String.valueOf(e.getCause()));
        if (state != State.INITIALIZING) {
            hub.getRegistry().failed(this);
        }
        done();
    }

    @Override
    protected boolean isDone() {
        return state == State.DONE;
    }

    @Override
    protected void launch() {
        LOGGER.debug("Launching phase {} for {} on {}.", state, type, poolName);

        switch (state) {
            case HANDLE_INACCESSIBLE:
            case WAIT_FOR_COMPLETION:
                /*
                 * Can be handled by the current thread.
                 */
                run();
                break;
            case INITIALIZING:
                running = hub.getPoolGroupInfoTaskExecutor().submit(this);
                break;
            case FIND_LOCATIONS:
            case SELECT_FOR_COPY:
            case SELECT_FOR_REMOVAL:
            case REPLICATION:
            case REDUCTION:
                running = hub.getPoolStatusChangeTaskExecutor().submit(this);
                break;
            case DONE:
                /*
                 * Shouldn't get here ...
                 */
                return;
            default:
                String message = String.format("Worker for %s on %s launched in "
                                                + "an illegal state %s.",
                                type, poolName, state);
                hub.getRegistry().failed(this);
                throw new IllegalStateException(message);
        }
    }

    @Override
    protected void nextState() {
        LOGGER.debug("Completed phase {} for {} on {}; calling next state.",
                        state, type, poolName);

        switch (state) {
            case INITIALIZING:
                state = State.WAIT_FOR_START;
                /*
                 * Start is triggered by the notifier,
                 * so there is nothing to launch/run here.
                 */
                break;
            case WAIT_FOR_START:
                state = State.FIND_LOCATIONS;
                launch();
                break;
            case FIND_LOCATIONS:
                switch(type) {
                    case DOWN:    state = State.HANDLE_INACCESSIBLE;   break;
                    case RESTART: state = State.SELECT_FOR_REMOVAL;    break;
                }
                launch();
                break;
            case HANDLE_INACCESSIBLE:
                state = State.SELECT_FOR_COPY;
                launch();
                break;
            case SELECT_FOR_COPY:
                state = State.REPLICATION;
                launch();
                break;
            case SELECT_FOR_REMOVAL:
                state = State.REDUCTION;
                launch();
                break;
            case REPLICATION:
                state = State.WAIT_FOR_COMPLETION;
                launch();
                break;
            case WAIT_FOR_COMPLETION:
            case REDUCTION:
                done();
                break;
            default:
                break;
        }
    }

    @Override
    protected void replicate() {
        doReplication(locationMap);
    }

    @Override
    protected void reduce() {
        toRemove = locationMap;
        doReduction();
    }

    @Override
    protected void register() {
        hub.getRegistry().register(this);
    }

    @Override
    protected void setDone() {
        state = State.DONE;
    }

    @Override
    protected void startNotifier() {
        long wait = hub.getPoolStatusChangeWindowUnit()
                       .toMillis(hub.getPoolStatusChangeWindow());
        PoolUpdateStatusNotifier notifier
                        = new PoolUpdateStatusNotifier(poolName,
                                                       this,
                                                       type,
                                                       wait);
        this.notifier = notifier;
        new Thread(notifier, notifier.getNotifierName()).start();
        hub.getPoolStatusCache().registerPoolNotifier(notifier);

        LOGGER.debug("Started notifier for PoolStatusUpdateWorker on {}.",
                        poolName);
    }

    /*
     * Single alarm for all files on the pool which have not been written
     * to backend storage (CUSTODIAL) but for which the pool contains
     * the unique copy.
     */
    private void handleInaccessible() {
        PnfsInfoCache cache = hub.getPnfsInfoCache();
        StringBuilder singletons = new StringBuilder();
        for (Iterator<PnfsId> it = pnfsIds.iterator(); it.hasNext();) {
            PnfsId pnfsId = it.next();
            PnfsIdInfo info = null;
            try {
                info = cache.getPnfsIdInfo(pnfsId);
            } catch (ExecutionException e) {
                LOGGER.warn("{}: {}, {}; skipping for now.",
                                pnfsId,
                                e.getMessage(),
                                String.valueOf(e.getCause()));
                it.remove();
                continue;
            }

            if (info.getAttributes().getRetentionPolicy()
                            == RetentionPolicy.CUSTODIAL) {
                continue;
            }

            if (info.getLocations().size() < 2) {
                singletons.append(pnfsId).append("\n");
                it.remove();
            }
        }

        if (singletons.length() > 0) {
            LOGGER.error(AlarmMarkerFactory.getMarker(PredefinedAlarm.INACCESSIBLE_FILE,
                                                      poolName),
                            INACCESSIBLE_FILE_MESSAGE,
                            poolName,
                            singletons);
        }
    }

    private void loadPnfsIdInfo() {
        loadPnfsIdInfo(Collections.EMPTY_SET);
    }

    private void selectForReduction() {
        Collection<String> toExclude = new ArrayList<>();
        toExclude.add(poolName);
        selectForReduction(pnfsIds, locationMap, toExclude);
    }

    private void selectForReplication() {
        selectForReplication(pnfsIds, locationMap, true);
    }
}
