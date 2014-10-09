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

import java.util.Iterator;
import java.util.List;

import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPoolGroup;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;

import dmg.cells.nucleus.CellEndpoint;

import org.dcache.replication.api.PnfsCacheMessageType;
import org.dcache.replication.api.ReplicationEndpoints;
import org.dcache.replication.api.ReplicationOperationRegistry;
import org.dcache.replication.api.ReplicationQueryUtilities;
import org.dcache.replication.api.ReplicationTaskExecutor;
import org.dcache.replication.data.PnfsIdMetadata;

/**
 * This task gathers all the necessary information needed to make a
 * determination about whether a given file (pnfisd) satisfies the
 * replication constraints given by the pool group of the pool it
 * has been written to, and/or by its storage class.
 *
 * @author arossi
 */
public final class VerifyPnfsIdTask extends PnfsIdTask {

    private final ReplicationTaskExecutor executor;
    private final ReplicationOperationRegistry map;

    public VerifyPnfsIdTask(PnfsIdMetadata opData,
                            ReplicationEndpoints hub,
                            ReplicationQueryUtilities utils,
                            ReplicationTaskExecutor executor,
                            ReplicationOperationRegistry map) {
        super(opData, hub, utils);
        this.executor = Preconditions.checkNotNull(executor);
        this.map = Preconditions.checkNotNull(map);
    }

    public Void call() throws Exception {
        try {
            /*
             * gather all the necessary data on replication constraints
             */
            opData.poolGroupData.constraints
                                .verifyConstraintsForFile(opData,
                                                          hub.getPnfsManager(),
                                                          hub.getEndpoint(),
                                                          hub.getPoolMonitor()
                                                          .getPoolSelectionUnit());

            LOGGER.debug("verifyConstraintsForFile {}: ({}--{},{},{}) / ({},{},{})}",
                            opData.pnfsId,
                            opData.poolGroupData.poolGroup.getName(),
                            opData.poolGroupData.poolGroup.getMinReplicas(),
                            opData.poolGroupData.poolGroup.getMaxReplicas(),
                            opData.poolGroupData.poolGroup.areSameHostReplicasEnabled(),
                            opData.poolGroupData.constraints.getMinimum(),
                            opData.poolGroupData.constraints.getMaximum(),
                            opData.poolGroupData.constraints.isSameHostOK());

            /*
             * figure out which queue it goes on
             */
            if (opData.poolGroupData.constraints.getMinimum() > 1) {
                verifyReplicas();
                LOGGER.debug(" MODE {}, greedy {}, current {}, required {}",
                                opData.getMode().toString(),
                                utils.isUseGreedyRequests(),
                                opData.getReplicaPools().size(),
                                opData.getReplicaDelta());

                switch (opData.getMode()) {
                    case REPLICATE:
                        if (opData.getSourceType().equals(PnfsCacheMessageType.SCAN)) {
                            opData.setSourceType(PnfsCacheMessageType.ADD);
                        }
                        executor.submitReplicateTask(opData);
                        break;
                    case REDUCE:
                        if (opData.getSourceType().equals(PnfsCacheMessageType.SCAN)) {
                            opData.setSourceType(PnfsCacheMessageType.CLEAR);
                        }
                        executor.submitReduceTask(opData);
                        break;
                    default:
                }
            } else {
                LOGGER.error("{}, minimum constraint = 1; unregistering.",
                             opData);
                map.unregister(opData);
            }
        } catch (Exception e) {
            LOGGER.error("{}, exception was thrown during verification: {}.",
                         opData, e.getMessage());
            map.unregister(opData);
            throw e;
        }

        return null;
    }

    /**
     * After determining which pools in the pool group are on-line,
     * gets the cache locations and returns only those for active pools
     * containing cached+system-sticky copies.
     */
    private void verifyReplicas() throws InterruptedException,
                                                 CacheException {
        SelectionPoolGroup poolGroup = opData.poolGroupData.poolGroup;
        PnfsId pnfsId = opData.pnfsId;
        String pnfsid = opData.pnfsId.toString();

        /*
         * Check to see how many of the group's pools are active.
         */
        List<String> activePools
            = utils.getActivePoolsInGroup(poolGroup, opData.poolGroupData.psu);

        /*
         * Find all copy locations in namespace.
         */
        List<String> cacheLocations = utils.getCacheLocations(pnfsId);

        CellEndpoint endpoint = hub.getEndpoint();

        /*
         * Remove all locations which are not active or
         * do not contain system sticky copies.
         */
        for (Iterator<String> it = cacheLocations.iterator(); it.hasNext();) {
            String location = it.next();
            if (!activePools.contains(location) ||
                !utils.isSystemSticky(pnfsid, location, endpoint)) {
                it.remove();
            }
        }

        LOGGER.debug("verifyReplicas {}: active pools {}; cached+sticky {}",
                        pnfsId,
                        activePools,
                        cacheLocations);

        opData.addReplicaPools(cacheLocations);
        opData.computeDelta(utils);
    }
}
