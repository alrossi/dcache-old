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
package org.dcache.replication.data;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;

import org.dcache.pool.repository.CacheEntry;
import org.dcache.replication.api.PnfsCacheMessageType;
import org.dcache.replication.api.ReplicationOperationMode;
import org.dcache.replication.api.ReplicationQueryUtilities;

/**
 * Data structure for holding the basic replication information for a given
 * file.
 *
 * @author arossi
 */
public final class PnfsIdMetadata {
    public final PnfsId pnfsId;
    public final String poolName;
    public final PoolGroupMetadata poolGroupData;


    /**
     * Acks from ReplicationStatusMessage.
     */
    public final AtomicInteger successAcknowledgments;

    private final List<String> replicaPools;

    private PnfsCacheMessageType sourceType;
    private ReplicationOperationMode mode;
    private int replicaDelta;

    /**
     * New locations from replication are added to {@link #replicaPools};
     * locations removed by reduce are removed from {@link #replicaPools};
     * this integer represents the original number of locations before
     * the operation began.
     */
    private int originalCount;

    public PnfsIdMetadata(PnfsId pnfsId,
                          String poolName,
                          PnfsCacheMessageType sourceType,
                          PoolGroupMetadata poolGroupData)
                    throws CacheException, InterruptedException {
        Preconditions.checkNotNull(pnfsId);
        this.pnfsId = pnfsId;
        this.poolName = poolName;
        this.sourceType = sourceType;
        this.poolGroupData = poolGroupData;
        replicaDelta = 0;
        successAcknowledgments = new AtomicInteger(0);
        replicaPools = new ArrayList<>();
        mode = ReplicationOperationMode.NONE;
    }

    /**
     * Note that this constructor (indirectly) calls the
     * {@link PoolSelectionUnit} to determine whether the pool group requires
     * replication. If not, the {@link #poolGroupData.poolGroup} field will
     * remain <code>null</code>.
     */
    public PnfsIdMetadata(PnfsId pnfsId,
                          String poolName,
                          PnfsCacheMessageType sourceType,
                          PoolSelectionUnit psu,
                          ReplicationQueryUtilities utils)
                    throws CacheException, InterruptedException {
        this(pnfsId,
             poolName,
             sourceType,
             new PoolGroupMetadata(poolName, psu, utils));
    }

    public void computeDelta(ReplicationQueryUtilities utils) {
        int numberOfReplicas = replicaPools.size();
        int maximum = poolGroupData.constraints.getMaximum();
        int minimum = poolGroupData.constraints.getMinimum();

        /*
         * The deficiency delta is expressed by
         * a negative number and the redundancy by a positive;
         * hence the former indicates the need for REPLICATE,
         * the latter for REDUCE.
         */
        replicaDelta
            = utils.isUseGreedyRequests() ? numberOfReplicas - maximum :
                                            numberOfReplicas - minimum;

        if (replicaDelta < 0) {
            mode = ReplicationOperationMode.REPLICATE;
        } else if (replicaDelta > 0) {
            mode = ReplicationOperationMode.REDUCE;
        } else {
            mode = ReplicationOperationMode.NONE;
        }

        originalCount = numberOfReplicas;
    }

    public synchronized void addReplicaPool(String pool) {
        replicaPools.add(pool);
    }

    public synchronized void addReplicaPools(List<String> replicas) {
        replicaPools.addAll(replicas);
    }

    public PnfsIdMetadata copy(String poolName)
                    throws CacheException, InterruptedException {
        PnfsIdMetadata copy = new PnfsIdMetadata(pnfsId,
                                                 poolName,
                                                 sourceType,
                                                 poolGroupData);
        copy.mode = mode;
        copy.replicaPools.addAll(replicaPools);
        copy.replicaDelta = replicaDelta;
        copy.successAcknowledgments.set(successAcknowledgments.get());
        copy.originalCount = replicaPools.size();
        return copy;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (!(obj instanceof PnfsIdMetadata)) {
            return false;
        }

        PnfsIdMetadata other = (PnfsIdMetadata)obj;

        if (!pnfsId.equals(other.pnfsId)) {
            return false;
        }

        if (!poolName.equals(other.poolName)) {
            return false;
        }

        return true;
    }

    public int getAbsoluteDelta() {
        return Math.abs(replicaDelta);
    }

    public ReplicationOperationMode getMode() {
        return mode;
    }

    public synchronized int getOriginalCount() {
        return originalCount;
    }

    public int getReplicaDelta() {
        return replicaDelta;
    }

    public synchronized List<String> getReplicaPools() {
        return new ArrayList<>(replicaPools);
    }

    public PnfsCacheMessageType getSourceType() {
        return sourceType;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(pnfsId, poolName);
    }

    public synchronized boolean hasReplica(String pool) {
        return replicaPools.contains(pool);
    }

    public boolean isRequestSatisfied() {
        return originalCount - replicaPools.size() == replicaDelta;
    }

    public synchronized void removeReplicaPool(String pool) {
        replicaPools.remove(pool);
    }

    public void setSourceType(PnfsCacheMessageType sourceType) {
        this.sourceType = sourceType;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName()
                      + "-"
                      + pnfsId
                      + "."
                      + poolName
                      + "("
                      + poolGroupData
                      + "):("
                      + sourceType
                      + ","
                      + mode
                      + ","
                      + replicaPools
                      + ","
                      + successAcknowledgments.get()
                      + ")";
    }
}
