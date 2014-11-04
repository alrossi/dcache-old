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
package org.dcache.replication.v3.namespace.tasks;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPool;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;

import org.dcache.replication.v3.namespace.ReplicaManagerHub;
import org.dcache.replication.v3.namespace.ResilientInfoCache;
import org.dcache.replication.v3.namespace.data.PoolGroupInfo;
import org.dcache.replication.v3.vehicles.RemoveReplicasMessage;

/**
 * Responsible for eliminating any excess copies of a single file.
 * This is done by communicating the removal to the pool repository
 * via a special message using a ListenableFuture.
 *
 * @author arossi
 */
public class SinglePnfsidReductionTask implements Runnable {
    private static final Logger LOGGER
        = LoggerFactory.getLogger(SinglePnfsidReductionTask.class);

    class RemoveResultListener implements Runnable {
        ListenableFuture<RemoveReplicasMessage> future;

        public void run() {
            RemoveReplicasMessage message = null;

            try {
                message = future.get();
            } catch (InterruptedException | ExecutionException t) {
                listeners.remove(this);
                hub.getReductionTaskHandler()
                   .taskFailed(SinglePnfsidReductionTask.this, t.getMessage());
                return;
            }

            if (future.isCancelled()) {
                hub.getReductionTaskHandler()
                   .taskCancelled(SinglePnfsidReductionTask.this);
            } else if (!message.iterator().hasNext()) {
                hub.getReductionTaskHandler()
                   .taskCompleted(SinglePnfsidReductionTask.this);
            } else if (message.retries < 2){
                removeReplicas(message);
                message.retries++;
            }

            listeners.remove(this);
        }
    }

    public final PnfsId pnfsId;

    private final ReplicaManagerHub hub;
    private final Set<String> confirmedLocations;
    private final Collection<RemoveResultListener> listeners;

    public SinglePnfsidReductionTask(PnfsId pnfsId,
                                     Collection<String> confirmedLocations,
                                     ReplicaManagerHub hub) {
        this.pnfsId = Preconditions.checkNotNull(pnfsId);
        this.hub = hub;
        this.confirmedLocations = new HashSet<>();
        this.confirmedLocations.addAll(confirmedLocations);
        this.listeners = new ArrayList<>();
    }

    public void run() {
        try {
            Collection<PnfsId> pnfsids = new ArrayList<>();
            pnfsids.add(pnfsId);

            ResilientInfoCache cache = hub.getCache();

            /*
             * If we are executing this task, confirmedLocations
             * cannot be empty.
             */
            String poolFromGroup = confirmedLocations.iterator().next();
            PoolGroupInfo info = cache.getPoolGroupInfo(poolFromGroup);
            Collection<SelectionPool> poolsInGroup = info.getPools();
            List<String> allLocations = hub.getCache()
                                           .getAllLocationsFor(pnfsId);

            for (String location: allLocations) {
                if (poolsInGroup.contains(location) &&
                                !confirmedLocations.contains(location)) {
                    removeReplicas(new RemoveReplicasMessage(location,
                                                             pnfsids));
                }
            }
        } catch (CacheException | ExecutionException t) {
            hub.getReductionTaskHandler()
                .taskFailed(SinglePnfsidReductionTask.this, t.getMessage());
        }
    }

    private void removeReplicas(RemoveReplicasMessage message) {
        RemoveResultListener listener = new RemoveResultListener();
        ListenableFuture<RemoveReplicasMessage> future
            = hub.getPoolStubFactory()
                 .getCellStub(message.pool).send(message);
        listener.future = future;
        future.addListener(listener,
                           hub.getPnfsInfoTaskExecutor());
        listeners.add(listener);
        LOGGER.trace("Sent SinglePnfsidReductionTask for {} to {}",
                        pnfsId, message.pool);
    }
}
