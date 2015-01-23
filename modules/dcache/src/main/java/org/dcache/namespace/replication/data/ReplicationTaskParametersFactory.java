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
package org.dcache.namespace.replication.data;

import java.util.concurrent.ExecutionException;

import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPoolGroup;
import diskCacheV111.poolManager.StorageUnit;
import diskCacheV111.util.PnfsId;
import org.dcache.namespace.replication.ReplicaManagerHub;
import org.dcache.namespace.replication.caches.PoolInfoCache;
import org.dcache.pool.migration.PoolSelectionStrategy;
import org.dcache.pool.migration.ProportionalPoolSelectionStrategy;
import org.dcache.pool.migration.TaskParameters;
import org.dcache.vehicles.FileAttributes;

/**
 * Convenience for populating the parameters of a migration task with
 * the correct replication constraints.
 *
 * Created by arossi on 1/15/15.
 */
public final class ReplicationTaskParametersFactory {
    private static final PoolSelectionStrategy PROPORTIONAL
                    = new ProportionalPoolSelectionStrategy();

    public static TaskParameters create(String pool,
                                        PnfsId pnfsId,
                                        FileAttributes attributes,
                                        ReplicaManagerHub hub) {
        PoolGroupInfo poolInfo;
        PoolInfoCache cache = hub.getPoolInfoCache();

        try {
            poolInfo = cache.getPoolGroupInfo(pool);
        } catch (ExecutionException t) {
            throw new RuntimeException(String.format("Fatal error prior to launching actual "
                                            + "migration task for %s@%s: %s, cause: %s",
                            pnfsId,
                            pool,
                            t.getMessage(),
                            t.getCause()));
        }

         /*
         * First determine replication contraints (number of copies required,
         * and whether replicas can be in different pools on the same host).
         */
        SelectionPoolGroup poolGroup = poolInfo.getPoolGroup();
        int minimum = poolGroup.getMinReplicas();
        int maximum = poolGroup.getMaxReplicas();
        String onlyOneCopyPer = poolGroup.getOnlyOneCopyPer(); // should be used in Task constructor TODO

        StorageUnit sunit = findStorageUnit(poolInfo, attributes);

        if (sunit != null) {
            Integer smax = sunit.getMaxReplicas();
            Integer smin = sunit.getMinReplicas();

            if (smax != null) { // both must be valid in this case
                maximum = smax;
                minimum = smin;
            }

            String oneCopyPer = sunit.getOnlyOneCopyPer();
            if (oneCopyPer != null) {
                onlyOneCopyPer = oneCopyPer;
            }
        }

        return new TaskParameters(hub.getPoolStubFactory().getCellStub(pool),
                                  hub.getPnfsManager(),
                                  hub.getPinManager(),
                                  hub.getMigrationTaskExecutor(),
                                  PROPORTIONAL,
                                  new CachedPoolList(hub.getPoolManagerPoolInfoCache(),
                                                     poolGroup.getName()),
                                  true, // eager
                                  false, // compute checksum on update
                                  false, // force source mode
                                  hub.isUseGreedyRequests() ? maximum : minimum); // LACKS onlyOneCopyPer TODO
    }

    private static StorageUnit findStorageUnit(PoolGroupInfo poolInfo,
                                               FileAttributes attributes) {
        String unitKey = attributes.getStorageClass();
        String hsm = attributes.getHsm();
        if (hsm != null) {
            unitKey += ("@" + hsm);
        }

        return poolInfo.getStorageUnit(unitKey);
    }
}
