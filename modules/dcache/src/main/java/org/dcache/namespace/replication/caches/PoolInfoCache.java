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
package org.dcache.namespace.replication.caches;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionLink;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPool;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPoolGroup;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionUnit;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionUnitGroup;
import diskCacheV111.poolManager.StorageUnit;
import diskCacheV111.util.CacheException;
import org.dcache.namespace.replication.data.PoolGroupInfo;
import org.dcache.poolmanager.PoolMonitor;

/**
 * Because replica handling may involve many calls to the pool monitor,
 * precautions must be taken to avoid DOS on this service.  This cache assumes
 * that the values asked for are reasonably stable within the limits defined
 * for the timeout.  All information needed for replica handling
 * concerning selection units should pass through this cache.
 * <p/>
 *
 * This class is thread-safe.  It is assumed that access to the cache
 * will be on a dedicated thread.
 * <p/>
 *
 * The injected monitor is provided by
 * org.dcache.poolmanager.RemotePoolMonitorFactoryBean, which means
 * it already has one level of caching incorporated into it.  We have
 * implemented this second level for further efficiency.
 *
 * Created by arossi on 1/22/15.
 */
public final class PoolInfoCache extends
                AbstractResilientInfoCache<String, PoolGroupInfo> {
    private PoolMonitor poolMonitor;

    /**
     * Calls cache.get().
     *
     * @param pool to get group info for
     * @return info object encapsulating pool group metadata, including
     *         storage groups associated with it.
     * @throws ExecutionException
     */
    public synchronized PoolGroupInfo getPoolGroupInfo(String pool)
                    throws ExecutionException {
        PoolGroupInfo info = cache.get(pool, ()->load(pool));
        if (info != null) {
            return info;
        }
        throw new NoSuchElementException(pool + " has no mapped information"
                                              + " for pool group.");
    }

    /**
     * Does not pass through cache.  Here for convenient access
     * to the pool monitor.
     *
     * @return set of all active pools.
     * @throws CacheException
     * @throws InterruptedException
     */
    public Collection<String> findAllActivePools()
                    throws CacheException, InterruptedException {
        PoolSelectionUnit psu = poolMonitor.getPoolSelectionUnit();
        return new HashSet<>(Arrays.asList(psu.getActivePools()));
    }

    public void setPoolMonitor(PoolMonitor poolMonitor) {
        this.poolMonitor = poolMonitor;
    }

    /**
     * Invalidates the cache and reloads all current entries.
     *
     * @throws Exception
     */
    public synchronized void reload() throws Exception {
        Collection<String> keys = cache.asMap().keySet();
        cache.invalidateAll();
        for (String pool: keys) {
            load(pool);
        }
    }

    @Override
    protected void prettyPrint(PoolGroupInfo value, StringBuilder builder) {
        value.getInfo(builder);
    }

    /*
     *  If the pool does not belong to a resilient group, the group
     *  field on the info object remains <code>null</code>.  Otherwise,
     *  the info object is populated with a list of pools and storage units
     *  in the group.
     */
    private PoolGroupInfo load(String poolName) throws Exception {
        PoolGroupInfo info = new PoolGroupInfo();
        getResilientPoolGroupOfPool(poolName, info);
        if (info.isResilient()) {
            String poolGroup = info.getPoolGroup().getName();
            Collection<SelectionPool> pools
                            = poolMonitor.getPoolSelectionUnit()
                                         .getPoolsByPoolGroup(poolGroup);

            getStorageUnitsInGroup(info, poolGroup);

            String[] active = poolMonitor.getPoolSelectionUnit()
                            .getActivePools();
            Set<String> valid = new HashSet<>(Arrays.asList(active));

            for (SelectionPool pool : pools) {
                String name = pool.getName();
                if (valid.contains(name)) {
                    info.addPool(pool);
                    if (!name.equals(poolName)) {
                        cache.put(pool.getName(), info);
                    }
                }
            }

            info.setMinMaxBounds();
        }
        return info;
    }

    /*
     *  Assumes only one resilient poolgroup per pool.
     */
    private void getResilientPoolGroupOfPool(String pool, PoolGroupInfo info) {
        Collection<SelectionPoolGroup> pgroups
                        = poolMonitor.getPoolSelectionUnit()
                                     .getPoolGroupsOfPool(pool);
        if (pgroups == null || pgroups.isEmpty()) {
            return;
        }

        Iterator<SelectionPoolGroup> it = pgroups.iterator();
        SelectionPoolGroup group;

        do {
            group = it.next();
            if (group.getMinReplicas() > 1) {
                info.setPoolGroup(group);
                break;
            }
        } while (it.hasNext());
    }

    private void getStorageUnitsInGroup(PoolGroupInfo info, String poolGroup) {
        Collection<SelectionLink> links
                        = poolMonitor.getPoolSelectionUnit()
                                     .getLinksPointingToPoolGroup(poolGroup);
        for (SelectionLink link : links) {
            Collection<SelectionUnitGroup> ugroups = link.getUnitGroupsTargetedBy();
            for (SelectionUnitGroup ugroup : ugroups) {
                Collection<SelectionUnit> units = ugroup.getMemeberUnits();
                for (SelectionUnit unit : units) {
                    if (unit instanceof StorageUnit) {
                        info.addStorageUnit((StorageUnit) unit);
                    }
                }
            }
        }
    }
}
