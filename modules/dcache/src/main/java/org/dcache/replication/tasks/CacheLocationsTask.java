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
package org.dcache.replication.tasks;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PnfsModifyCacheLocationMessage;

import org.dcache.alarms.AlarmMarkerFactory;
import org.dcache.alarms.Severity;
import org.dcache.replication.api.ReplicationEndpoints;
import org.dcache.replication.api.ReplicationMessageReceiver;
import org.dcache.replication.api.ReplicationQueryUtilities;
import org.dcache.replication.api.ReplicationScanType;
import org.dcache.replication.data.PnfsIdMetadata;
import org.dcache.replication.data.PoolMetadata;
import org.dcache.vehicles.replication.ListPnfsidsForPoolMessage;
import org.dcache.vehicles.replication.PnfsScannedCacheLocationsMessage;

/**
 * Involves a query to the namespace which will return a listing of all PnfsIds
 * for which a {@link PnfsIdRequestTask} may need to be issued.
 * <p>
 *
 * The task is two-phase.  In the first, its {@link #call()} method is invoked,
 * sending an asynchronous request for the (pnfsid,location) listings.  In
 * the second, the {@link #run()} method processes the list.  The second
 * execution of the task is in conjunction with a ListenableFuture bound to
 * the first.
 *
 * @author arossi
 */
public abstract class CacheLocationsTask extends BaseReplicationTask
                        implements Runnable {
    protected static final String SCANS = "TOTAL POOL SCANS";

    protected final PoolMetadata poolData;
    protected final ReplicationMessageReceiver handler;
    protected final ReplicationScanType type;

    protected Executor executor;
    protected ListenableFuture<? extends ListPnfsidsForPoolMessage> future;

    protected CacheLocationsTask(PoolMetadata poolData,
                                 ReplicationEndpoints hub,
                                 ReplicationQueryUtilities utils,
                                 ReplicationMessageReceiver handler,
                                 ReplicationScanType type) {
        super(hub, utils);
        this.poolData = Preconditions.checkNotNull(poolData);
        this.handler = Preconditions.checkNotNull(handler);
        this.type = Preconditions.checkNotNull(type);
    }

    public Void call() throws Exception {
        ListPnfsidsForPoolMessage message = utils.getListMessage(poolData,
                                                                 type,
                                                                 true);

        LOGGER.debug("Scan for inadequate copies for pool {}, "
                        + "sending message {}.",
                        poolData.pool,
                        message);

        /*
         * should not return <code>null</code>
         */
        future = hub.getPnfsManager().send(message);
        future.addListener(this, executor);
        return null;
    }

    public void run() {
        LOGGER.debug("run() called for {}, future is done: {}", poolData,
                                                                future.isDone());
        if (future.isCancelled()) {
            LOGGER.warn("Request replicas for pool {} was cancelled.",
                            poolData.pool);
        } else {
            processReply();
        }
    }

    public void setExecutor(Executor executor) {
        this.executor = executor;
    }

    private void processReply() {
        try {
            ListPnfsidsForPoolMessage message = future.get();
            LOGGER.trace("received {}", message);

            if (message.getErrorObject() != null) {
                throw new ExecutionException
                    (new Throwable(message.getErrorObject().toString()));
            }

            Multimap<String, String> pnfsidToLocations = message.getReplicas();

            ImmutableList<String> excluded = message.getExcludedLocations();
            String pool = message.location;
            if (!poolData.pool.equals(pool)) {
                throw new IllegalArgumentException("the pool of the returned message, "
                                                   + pool
                                                   + ", does not match the pool "
                                                   + "for this task, "
                                                   + poolData.pool
                                                   + "; this is likely a bug.");
            }

            long alarms = 0;

            for (String pnfsid : pnfsidToLocations.keySet()) {
                if (poolData.poolGroupData.visitedPnfsids.contains(pnfsid)) {
                    continue;
                }

                poolData.poolGroupData.visitedPnfsids.add(pnfsid);

                List<String> locations
                    = new ArrayList(pnfsidToLocations.get(pnfsid));

                LOGGER.debug("pnfsid {}, locations {}", pnfsid, locations);

                /*
                 * Remove inactive pools.
                 */
                if (excluded != null) {
                    for (String down: excluded) {
                        locations.remove(down);
                    }
                }

                /*
                 * Remove current from list if it is inaccessible (may
                 * be rendundant, but just for insurance).
                 */
                if (ReplicationScanType.DOWN.equals(type)) {
                    locations.remove(poolData.pool);
                }

                if (locations.isEmpty()) {
                    ++alarms;
                    continue;
                }

                String source = utils.removeRandomEntry(locations);

                PnfsModifyCacheLocationMessage cacheLocationMsg
                    = new PnfsScannedCacheLocationsMessage(new PnfsId(pnfsid),
                                                           source);
                handler.messageArrived(cacheLocationMsg);
            }

            if (alarms > 0) {
                LOGGER.error(AlarmMarkerFactory.getMarker(Severity.HIGH,
                                                          PnfsIdMetadata.ALARM_INACCESSIBLE,
                                                          poolData.pool),
                            "Pool {} is DOWN and contains {} inaccessible files; "
                            + "run the replica manager admin command "
                            + "'ls ua {}' for a full list "
                            + "of files in need of recovery.",
                            poolData.pool,
                            alarms,
                            poolData.pool);
            }
        } catch (InterruptedException | ExecutionException t) {
                LOGGER.error("Request replicas for pool {} "
                            + "could not process reply.",
                            poolData.pool, t.getMessage());
        }
    }
}
