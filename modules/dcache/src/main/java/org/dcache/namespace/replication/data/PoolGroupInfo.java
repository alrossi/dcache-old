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

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPool;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPoolGroup;
import diskCacheV111.poolManager.StorageUnit;

/**
 * Encapsulates the pool group data obtainable from the pool selection unit.
 * This includes a map of all storage unit types found in the pool group.
 *
 * Created by arossi on 1/29/15.
 */
public class PoolGroupInfo implements Serializable {
    private static final long serialVersionUID = 1L;

    private final Map<String, StorageUnit> storageUnits;
    private final Set<SelectionPool> pools;

    private SelectionPoolGroup poolGroup;
    private int minUpperBound;
    private int maxLowerBound;

    public PoolGroupInfo() {
        storageUnits = new HashMap<>();
        pools = new HashSet<>();
    }

    public void addPool(SelectionPool pool) {
        pools.add(pool);
    }

    public void addStorageUnit(StorageUnit storageUnit) {
        storageUnits.put(storageUnit.getName(), storageUnit);
    }

    public void getInfo(StringBuilder builder) {
        if (isResilient()) {
            builder.append(poolGroup.getName())
                   .append("\n\t(min ").append(poolGroup.getMinReplicas())
                   .append(")\n\t(max ").append(poolGroup.getMaxReplicas())
                   .append(")\n\t(1per ").append(poolGroup.getOnlyOneCopyPer())
                   .append(")\n\t(pools ").append(getPoolNames())
                   .append(")\n\t(sunits ").append(getStorageUnitNames());
        } else {
            builder.append("not resilient");
        }
    }

    public int getLowerBoundForMax() {
        return maxLowerBound;
    }

    public int getUpperBoundForMin() {
        return minUpperBound;
    }

    public SelectionPoolGroup getPoolGroup() {
        return poolGroup;
    }

    public Collection<SelectionPool> getPools() {
        return pools;
    }

    public Set<String> getPoolNames() {
        Set<String> names = new HashSet<>();
        if (pools != null) {
            for (SelectionPool p : pools) {
                names.add(p.getName());
            }
        }
        return names;
    }

    public StorageUnit getStorageUnit(String unitName) {
        if (storageUnits == null) {
            return null;
        }
        return storageUnits.get(unitName);
    }

    public boolean isResilient() {
        return poolGroup != null;
    }

    public void setPoolGroup(SelectionPoolGroup poolGroup) {
        this.poolGroup = poolGroup;
    }

    public Iterator<StorageUnit> storageUnits() {
        if (storageUnits == null) {
            Collection<StorageUnit> empty = Collections.EMPTY_LIST;
            return empty.iterator();
        }
        return storageUnits.values().iterator();
    }

    public void setMinMaxBounds() {
        minUpperBound = poolGroup.getMinReplicas();
        maxLowerBound = poolGroup.getMaxReplicas();
        Integer unitMin;
        Integer unitMax;

        for (Iterator<StorageUnit> it = storageUnits(); it.hasNext(); ) {
            StorageUnit unit = it.next();
            unitMin = unit.getMinReplicas();
            unitMax = unit.getMaxReplicas();
            if (unitMin != null) {
                minUpperBound = Math.max(minUpperBound, unitMin);
            }
            if (unitMax != null) {
                maxLowerBound = Math.min(maxLowerBound, unitMax);
            }
        }
    }

    private Collection<String> getStorageUnitNames() {
        if (storageUnits == null) {
            return Collections.EMPTY_SET;
        }
        return storageUnits.keySet();
    }
}
