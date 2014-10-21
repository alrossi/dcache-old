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
package org.dcache.replication.util;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.MissingResourceException;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPool;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPoolGroup;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PnfsGetCacheLocationsMessage;

import dmg.cells.nucleus.CellEndpoint;

import org.dcache.cells.CellStub;
import org.dcache.pool.repository.EntryState;
import org.dcache.replication.api.ReplicationEndpoints;
import org.dcache.replication.api.ReplicationQueryUtilities;
import org.dcache.replication.api.ReplicationScanType;
import org.dcache.replication.api.ReplicationTaskExecutor;
import org.dcache.replication.data.PoolGroupMetadata;
import org.dcache.replication.data.PoolMetadataV1;
import org.dcache.vehicles.replication.ListPnfsidsForPoolMessage;
import org.dcache.vehicles.replication.PoolCopyIsSystemStickyMessage;

/**
 * Various shared procedures for pool selection and location queries.
 *
 * @author arossi
 */
public final class ReplicationQueryUtilitiesImpl implements ReplicationQueryUtilities {
    private static final Logger LOGGER
        = LoggerFactory.getLogger(ReplicationQueryUtilitiesImpl.class);

    private static final Random random = new Random(System.currentTimeMillis());

    private ReplicationTaskExecutor executor;
    private ReplicationEndpoints hub;
    private boolean useGreedyRequests;

    public List<String> getActivePoolsInGroup(SelectionPoolGroup poolGroup,
                                              PoolSelectionUnit psu) {
        List<String> active = new ArrayList<>();
        getPoolsInGroup(poolGroup, psu, null, active, null);
        return active;
    }

    public List<String> getCacheLocations(PnfsId pnfsId)
                    throws CacheException, InterruptedException {
        PnfsGetCacheLocationsMessage msg = new PnfsGetCacheLocationsMessage(pnfsId);
        msg = hub.getPnfsManager().sendAndWait(msg);

        if (msg == null) {
            return Collections.EMPTY_LIST;
        }

        return msg.getCacheLocations();
    }

    public ListPnfsidsForPoolMessage getListMessage(PoolMetadataV1 data,
                                                    ReplicationScanType type,
                                                    boolean activeOnly)
                    throws CacheException,
                           InterruptedException,
                           TimeoutException {
        PoolGroupMetadata pgData = data.poolGroupData;
        int minimum;
        int maximum;

        if (ReplicationScanType.UNIQUE.equals(type)) {
            minimum = 2;
            maximum = 2;
        } else {
            pgData.constraints.verifyConstraintsForPoolGroup(pgData.poolGroup,
                                                             pgData.psu);
            minimum = pgData.constraints.getMinimum();
            maximum = pgData.constraints.getMaximum();
        }

        /*
         * Always override the activeOnly flag to true on
         * DOWN/RESTART/PERIODIC types.
         */
        switch(type) {
            case DOWN:
            case RESTART:
            case PERIODIC:
                activeOnly = true;
                break;
            default:
        }

        List<String> inactive = null;

        if (activeOnly) {
            inactive = new ArrayList();
            getPoolsInGroup(pgData.poolGroup, pgData.psu, null, null, inactive);
        }

        return getListMessage(data.pool, type, minimum, maximum, inactive);
    }

    public ListPnfsidsForPoolMessage getListMessage(String pool,
                                                    ReplicationScanType type,
                                                    int minimum,
                                                    int maximum,
                                                    List<String> exclude) {
        /*
         * Make sure the down pool is listed as inactive
         * so that the location information in the namespace
         * for that pool is not included in the count filter.
         */
        if (ReplicationScanType.DOWN.equals(type)) {
            exclude.add(pool);
        }

        ListPnfsidsForPoolMessage message
            = new ListPnfsidsForPoolMessage(pool, minimum, maximum, type);
        message.setExcludedLocations(exclude);
        return message;
    }

    public void getPoolsInGroup(SelectionPoolGroup poolGroup,
                                PoolSelectionUnit psu,
                                List<String> pools,
                                List<String> activePools,
                                List<String> inactivePools) {
        Collection<SelectionPool> selectionPools
            = psu.getPoolsByPoolGroup(poolGroup.getName());

        Set<String> poolsInGroup = new HashSet<>();

        for (SelectionPool pool : selectionPools) {
            poolsInGroup.add(pool.getName());
        }

        if (pools != null) {
            pools.addAll(poolsInGroup);
        }

        if (activePools == null && inactivePools == null) {
            return;
        }

        String[] allActive = psu.getActivePools();

        Set<String> groupActive = new HashSet<>();

        for (String pool : allActive) {
            if (poolsInGroup.contains(pool)) {
                groupActive.add(pool);
            }
        }

        if (activePools != null) {
            activePools.addAll(groupActive);
        }

        if (inactivePools != null) {
            for (String pool : poolsInGroup) {
                if (!groupActive.contains(pool)) {
                    inactivePools.add(pool);
                }
            }
        }
    }

    public SelectionPoolGroup getResilientPoolGroupOfPool(String pool,
                                                          PoolSelectionUnit psu)
                    throws CacheException, InterruptedException {
        Preconditions.checkNotNull(pool);
        Preconditions.checkNotNull(psu);
        Collection<SelectionPoolGroup> pgroups = psu.getPoolGroupsOfPool(pool);
        if (pgroups == null || pgroups.isEmpty()) {
            return null;
        }

        Iterator<SelectionPoolGroup> it = pgroups.iterator();
        SelectionPoolGroup group;

        do {
            group = it.next();
            if (group.getMinReplicas() > 1) {
                return group;
            }
        } while (it.hasNext());

        return null;
    }



    public boolean isUseGreedyRequests() {
        return useGreedyRequests;
    }

    public String removeRandomEntry(List<String> list) {
        if (list.isEmpty()) {
            return null;
        }
        int index = Math.abs(random.nextInt()) % list.size();
        return list.remove(index);
    }

    public void scanActivePools(SelectionPoolGroup poolGroup,
                                PoolSelectionUnit psu) {
        List<String> active = getActivePoolsInGroup(poolGroup, psu);

        PoolGroupMetadata pgroupData
            = new PoolGroupMetadata(poolGroup, psu, this);

        for (String activePool : active) {
            LOGGER.debug("Pool {} of group {} is active; proceeding with scan",
                            activePool,
                            poolGroup.getName());

            PoolMetadataV1 poolData = new PoolMetadataV1(activePool, pgroupData);

            /*
             * Since this method should be called only if the pool group is
             * resilient, we can submit a scan task without further checking.
             */
            try {
                executor.submitScanFullTask(poolData);
            } catch (CacheException | InterruptedException t) {
                LOGGER.warn("{}: problem submitting scan task; this pool will"
                                + " be skipped.",
                                poolData,
                                t.getMessage());
            }
        }
    }

    public void setExecutor(ReplicationTaskExecutor executor) {
        this.executor = executor;
    }

    public void setHub(ReplicationEndpoints hub) {
        this.hub = hub;
    }

    public void setUseGreedyRequests(boolean useGreedyRequests) {
        this.useGreedyRequests = useGreedyRequests;
    }
}
