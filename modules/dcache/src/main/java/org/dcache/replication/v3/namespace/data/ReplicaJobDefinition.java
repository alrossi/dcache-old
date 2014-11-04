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
package org.dcache.replication.v3.namespace.data;

import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPoolGroup;
import diskCacheV111.poolManager.StorageUnit;
import diskCacheV111.vehicles.StorageInfo;

import org.dcache.cells.CellStub;
import org.dcache.pool.migration.CacheEntryMode;
import org.dcache.pool.migration.JobDefinition;
import org.dcache.pool.migration.PoolListByNames;
import org.dcache.pool.migration.PoolListByPoolGroup;
import org.dcache.pool.migration.ProportionalPoolSelectionStrategy;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.vehicles.FileAttributes;

/**
 * The creation of the definition entails first checking for
 * the minimum and maximum numbers, along with whether copies are
 * allowed on different pools on the same host.  The greedy flag
 * determines whether to associate the required replica count
 * with the minimum or the maximum.
 * <p>
 * Any values associated with the file's storage unit override
 * those for the pool group.
 *
 * @author arossi
 */
public class ReplicaJobDefinition extends JobDefinition {
    private static final CacheEntryMode REPLICA_CACHE_ENTRY_MODE =
                    new CacheEntryMode(CacheEntryMode.State.CACHED,
                                       ImmutableList.of(new StickyRecord("system",
                                                        StickyRecord.NON_EXPIRING)));

    public static ReplicaJobDefinition create(PoolGroupInfo message,
                                              FileAttributes fileAttributes,
                                              String pool,
                                              boolean greedy,
                                              CellStub poolManager) {
        /*
         * First determine replication contraints (number of copies required,
         * and whether replicas can be in different pools on the same host).
         */
        SelectionPoolGroup group = message.getPoolGroup();
        int minimum = group.getMinReplicas();
        int maximum = group.getMaxReplicas();
        boolean isSameHostOK = group.areSameHostReplicasEnabled();

        StorageInfo storageInfo = fileAttributes.getStorageInfo();
        String unitKey = storageInfo.getStorageClass();
        String hsm = storageInfo.getHsm();
        if (hsm != null) {
            unitKey += ("@" + hsm);
        }

        StorageUnit sunit = message.getStorageUnit(unitKey);

        if (sunit != null) {
            Integer smax = sunit.getMaxReplicas();
            Integer smin = sunit.getMinReplicas();

            if (smax != null) { // both should be valid
                maximum = smax;
                minimum = smin;
            }

            Boolean sEnabled = sunit.areSameHostReplicasEnabled();
            if (sEnabled != null) {
                isSameHostOK = sEnabled;
            }
        }

        int replicas = greedy ? maximum : minimum;

        return new ReplicaJobDefinition(ImmutableList.of(pool),
                                        ImmutableList.of(group.getName()),
                                        poolManager,
                                        REPLICA_CACHE_ENTRY_MODE,
                                        replicas,
                                        isSameHostOK);
    }

    private ReplicaJobDefinition(Collection<String> source,
                                 Collection<String> poolGroup,
                                 CellStub poolManager,
                                 CacheEntryMode targetMode,
                                 int replicas,
                                 /*
                                  * this needs to be taken care of; e.g.,
                                  * exclude-when=
                                  */
                                 boolean isSameHostOK) {
        super(null,       // filters
              null,       // source mode check is part of replica manager logic
              targetMode, // always CACHED+system(sticky)
              new ProportionalPoolSelectionStrategy(),
              null,       // cache entry comparator not necessary
              new PoolListByNames(poolManager, source),
              new PoolListByPoolGroup(poolManager, poolGroup),
              TimeUnit.MINUTES.toMillis(10),
              false,      // job is transient
              true,       // replica creation is always "eager"
              replicas,   // what is required, without consideration of what may exist
              false,      // only pin on a new replica copy should belong to system
              true,       // not sure whether the checksum is necessary ... for now set to true
              null,
              null,
              false);     // source mode check is part of replica manager logic
    }
}
