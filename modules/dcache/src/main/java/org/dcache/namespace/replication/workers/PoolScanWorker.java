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

import java.util.Collection;
import java.util.Collections;

import diskCacheV111.util.PnfsId;
import org.dcache.namespace.replication.ReplicaManagerHub;
import org.dcache.namespace.replication.caches.PnfsInfoCache;

/**
 * A worker responsible for all phases of the handling of a pool scan.
 * <p/>
 * Runs on a single thread, and joins on any PnfsUpdateWorkers it spawns.
 * Even though the state machine structure is unnecessary for this component,
 * it has been retained for the sake of uniformity among workers.
 * <p/>
 * A check is first done to make sure the source pool belongs to a resilient
 * group.  The pool group information is retrieved via a cache based on a periodic
 * refresh of the pool monitor. If resilient, the worker initializes and
 * registers a notifier which blocks other status change messages
 * from being processed while the scan in is progress.
 * <p/>
 * The first thing the worker does is to execute a query which returns
 * all locations for all pnfsIds found on this pool. These are then checked
 * against the pnfsids which have already been processed. Previously seen
 * pnfsids (during the current scan) are removed from the list and their
 * cache entry invalidated.
 * <p/>
 * A constraint check is then run for deficient replicas AND redundant replicas,
 * such that the list of pnfsids is parsed into two separate maps.  In checking
 * constraints, the current pool is not excluded from the list (unlike with
 * a pool status change update).
 * <p/>
 * The two sets of location-to-pnfsid multimaps are then processed.  Reduction
 * is run while the replication workers are running concurrently.  The final
 * phase (when reduction completes) is to wait for all the migration tasks
 * to complete. As is usual, all such removes are preceded by
 * the pinning of all locations for all the pnfsIds involved.
 * <p/>
 *
 * Created by arossi on 1/29/15.
 */
public class PoolScanWorker extends PoolUpdateWorker {
    enum State {
        START,               // check for resilience, register notifier
        FIND_LOCATIONS,      // call database to get map of {(pnfsid: replicas)}
                             // remove pnfsids that have been seen, add all the rest
        SELECT_FOR_COPY,     // bin the pnfsIds according to chosen sources
        SELECT_FOR_REMOVAL,  // bin the pnfsIds according to copies to remove
        REPLICATION,         // launch a PnfsUpdateWorker for each
        REDUCTION,           // pin all, remove selected, unpin remainder
        WAIT_FOR_COMPLETION, // join on the PnfsUpdateWorkers
        DONE                 // notify and unregister
    }

    /**
     * Binning for source locations selected for replication.
     */
    private final Multimap<String, PnfsId> toCopy = ArrayListMultimap.create();

    /**
     * Binning for source locations selected for reduction.
     */
    private final Multimap<String, PnfsId> toRemove = ArrayListMultimap.create();

    /**
     * A scan optimization.  Avoids repeated checks on same pnfsid from
     * one pool scan to the next.
     */
    private final Collection<String> seenPnfsids;

    private State state = State.START;

    public PoolScanWorker(String poolName,
                          Collection<String> seenPnfsids,
                          ReplicaManagerHub hub) {
        super(poolName, hub);
        this.seenPnfsids = seenPnfsids;
    }

    @Override
    public void run() {
        switch (state) {
            case START:                 register();
                                        getPoolGroupInfo();
                                        startNotifier();        nextState(); break;
            case FIND_LOCATIONS:        loadAndUpdatePnfsIds(); nextState(); break;
            case SELECT_FOR_COPY:       selectForReplication(); nextState(); break;
            case SELECT_FOR_REMOVAL:    selectForReduction();   nextState(); break;
            case REPLICATION:           replicate();            nextState(); break;
            case REDUCTION:             reduce();               nextState(); break;
            case WAIT_FOR_COMPLETION:   waitForCompletion();    nextState(); break;
            default:                                                         break;
        }
    }

    @Override
    protected void failed(Exception e) {
        LOGGER.error(ABORT_MESSAGE, "scan", poolName, state,
                        e == null ? "" : e.getMessage(),
                        e == null ? "" : String.valueOf(e.getCause()));
        hub.getRegistry().failed(this);
        done();
    }

    @Override
    protected boolean isDone() {
        return state == State.DONE;
    }

    /*
     * This worker is all run on the scan executor thread (scan workers are
     * run sequentially).  It will, however, spawn PnfsUpdateWorkers
     * whose threading and concurrency will be as usual.  Hence the
     * final stage on this thread is to wait for completion of those child
     * workers.
     */
    @Override
    protected void launch() {
        LOGGER.debug("Launching phase {} for scan on {}.", state, poolName);

        switch (state) {
            case START:
            case FIND_LOCATIONS:
            case SELECT_FOR_COPY:
            case SELECT_FOR_REMOVAL:
            case REPLICATION:
            case REDUCTION:
            case WAIT_FOR_COMPLETION:
                run();
                break;
            case DONE:
                /*
                 * Shouldn't get here ...
                 */
                return;
            default:
                String message = String.format("Scan worker for on %s launched "
                                                + "in an illegal state %s.",
                                poolName, state);
                hub.getRegistry().failed(this);
                throw new IllegalStateException(message);
        }
    }

    @Override
    protected void nextState() {
        LOGGER.debug("Completed phase {} for scan on {}; calling next state.",
                        state, poolName);

        switch (state) {
            case START:
                state = State.FIND_LOCATIONS;       launch();   break;
            case FIND_LOCATIONS:
                state = State.SELECT_FOR_COPY;      launch();   break;
            case SELECT_FOR_COPY:
                state = State.SELECT_FOR_REMOVAL;   launch();   break;
            case SELECT_FOR_REMOVAL:
                state = State.REPLICATION;          launch();   break;
            case REPLICATION:
                state = State.REDUCTION;            launch();   break;
            case REDUCTION:
                state = State.WAIT_FOR_COMPLETION;  launch();   break;
            case WAIT_FOR_COMPLETION:
                                                    done();     break;
            default:
                                                                break;
        }
    }

    @Override
    protected void replicate() {
        doReplication(toCopy);
    }

    @Override
    protected void reduce() {
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
        notifier = new PoolScanStatusNotifier(poolName, this);
        hub.getPoolStatusCache().registerPoolNotifier(notifier);

        LOGGER.debug("Started notifier for PoolScanWorker on {}.", poolName);
    }

    private void loadAndUpdatePnfsIds() {
        loadPnfsIdInfo(seenPnfsids);
        PnfsInfoCache cache = hub.getPnfsInfoCache();
        for (PnfsId pnfsId: pnfsIds) {
            seenPnfsids.add(pnfsId.toString());
        }
    }

    private void selectForReduction() {
        selectForReduction(pnfsIds, toCopy, Collections.EMPTY_LIST);
    }

    private void selectForReplication() {
        selectForReplication(pnfsIds, toCopy, false);
    }
}
