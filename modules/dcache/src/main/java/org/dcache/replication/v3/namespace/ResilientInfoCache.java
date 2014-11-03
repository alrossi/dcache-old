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
package org.dcache.replication.v3.namespace;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import diskCacheV111.namespace.NameSpaceProvider;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionLink;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPool;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPoolGroup;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionUnit;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionUnitGroup;
import diskCacheV111.poolManager.StorageUnit;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;

import org.dcache.auth.Subjects;
import org.dcache.namespace.FileAttribute;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.replication.v3.namespace.data.PoolGroupInfo;
import org.dcache.vehicles.FileAttributes;

import static org.dcache.namespace.FileAttribute.*;

/**
 * @author arossi
 */
public class ResilientInfoCache {
    private class PnfsInfoFetcher extends CacheLoader<PnfsId,
                                                      FileAttributes> {
        @Override
        public FileAttributes load(PnfsId pnfsId) throws Exception {
            return namespace.getFileAttributes(Subjects.ROOT,
                                               pnfsId,
                                               requiredAttributes);
        }
    }

    private class PoolInfoFetcher extends CacheLoader<String, PoolGroupInfo> {

        @Override
        public PoolGroupInfo load(String pool) throws Exception {
            PoolGroupInfo info = new PoolGroupInfo();
            getResilientPoolGroupOfPool(pool, info);
            if (info.isResilient()) {
                getAllPoolsOfGroup(info);
                getStorageUnitsInGroup(info);
            }
            loadAllPoolsInGroup(pool, info);
            return info;
        }

        private void getResilientPoolGroupOfPool(String pool,
                                                 PoolGroupInfo info) {
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

        private void getAllPoolsOfGroup(PoolGroupInfo info) {
            Collection<SelectionPool> pools =
                            poolMonitor.getPoolSelectionUnit()
                            .getPoolsByPoolGroup(info.getPoolGroup().getName());
            info.setPools(pools);
        }

        private void getStorageUnitsInGroup(PoolGroupInfo info) {
            Collection<SelectionLink> links
                = poolMonitor.getPoolSelectionUnit()
                             .getLinksPointingToPoolGroup(info.getPoolGroup()
                                                              .getName());
            for (SelectionLink link : links) {
                Collection<SelectionUnitGroup> ugroups
                    = link.getUnitGroupsTargetedBy();
                for (SelectionUnitGroup ugroup : ugroups) {
                    Collection<SelectionUnit> units = ugroup.getMemeberUnits();
                    for (SelectionUnit unit : units) {
                        if (unit instanceof StorageUnit) {
                            info.addStorageUnit((StorageUnit)unit);
                        }
                    }
                }
            }
        }

        private void loadAllPoolsInGroup(String source, PoolGroupInfo info) {
            Collection<SelectionPool> pools
                = poolMonitor.getPoolSelectionUnit()
                             .getPoolsByPoolGroup(info.getPoolGroup().getName());
            for (SelectionPool pool: pools) {
                if (source.equals(pool.getName())) {
                    continue;
                }
                poolInfoCache.put(pool.getName(), info);
            }
        }
    }

    private static final Set<FileAttribute> requiredAttributes
        = Collections.unmodifiableSet(EnumSet.of(ACCESS_LATENCY,
                                                 STORAGECLASS,
                                                 HSM));

    /*
     * Provided by org.dcache.poolmanager.RemotePoolMonitorFactoryBean
     * see resource pinmanager.xml for example
     */
    private PoolMonitor poolMonitor;

    private NameSpaceProvider namespace;

    private int pnfsInfoCacheExpiry = 1;
    private int poolInfoCacheExpiry = 1;
    private int poolStatusCacheExpiry = 1;
    private int pnfsInfoCacheSize = 1000;
    private int poolInfoCacheSize = 1000;
    private int poolStatusCacheSize = 50;
    private TimeUnit pnfsInfoCacheExpiryUnit = TimeUnit.MINUTES;
    private TimeUnit poolInfoCacheExpiryUnit = TimeUnit.MINUTES;
    private TimeUnit poolStatusCacheExpiryUnit = TimeUnit.HOURS;

    /*
     * From initialization.
     */
    private LoadingCache<PnfsId, FileAttributes> pnfsInfoCache;
    private LoadingCache<String, PoolGroupInfo> poolInfoCache;
    private Cache<String, String> poolStatusCache;

    public List<String> getAllLocationsFor(PnfsId pnfsId) throws CacheException {
        return namespace.getCacheLocation(Subjects.ROOT, pnfsId);
    }

    public FileAttributes getAttributes(PnfsId pnfsId)
                    throws ExecutionException {
        FileAttributes attributes = pnfsInfoCache.get(pnfsId);
        if (attributes != null) {
            return attributes;
        }
        throw new NoSuchElementException(pnfsId.toString()
                        + " has no mapped attributes.");
    }

    public PoolGroupInfo getPoolGroupInfo(String pool)
                    throws ExecutionException {
        PoolGroupInfo info = poolInfoCache.get(pool);
        if (info != null) {
            return info;
        }
        throw new NoSuchElementException(pool
                        + " has no mapped information for pool group.");
    }

    public boolean hasRecentlyChangedStatus(String pool, String status) {
        boolean present = poolStatusCache.getIfPresent(pool) != null;
        poolStatusCache.put(pool, status);
        return present;
    }

    public void clearPoolStatusCache() {
        poolStatusCache.invalidateAll();
    }

    public void initialize() throws IllegalArgumentException {
        if (pnfsInfoCacheExpiry < 0) {
            throw new IllegalArgumentException("Cache life must be positive "
                            + "integer; was: "
                            + pnfsInfoCacheExpiry);
        }

        if (poolInfoCacheExpiry < 0) {
            throw new IllegalArgumentException("Cache life must be positive "
                            + "integer; was: "
                            + poolInfoCacheExpiry);
        }

        if (pnfsInfoCacheSize < 1) {
            throw new IllegalArgumentException("Cache size must be non-zero "
                            + "positive integer; was: "
                            + pnfsInfoCacheSize);
        }

        if (poolInfoCacheSize < 1) {
            throw new IllegalArgumentException("Cache size must be non-zero "
                            + "positive integer; was: "
                            + poolInfoCacheSize);
        }

        pnfsInfoCache = CacheBuilder.newBuilder()
                        .expireAfterWrite(pnfsInfoCacheExpiry,
                                          pnfsInfoCacheExpiryUnit)
                        .maximumSize(pnfsInfoCacheSize)
                        .softValues()
                        .build(new PnfsInfoFetcher());

        poolInfoCache = CacheBuilder.newBuilder()
                        .expireAfterWrite(poolInfoCacheExpiry,
                                          poolInfoCacheExpiryUnit)
                        .maximumSize(poolInfoCacheSize)
                        .softValues()
                        .build(new PoolInfoFetcher());

        poolStatusCache = CacheBuilder.newBuilder()
                        .expireAfterWrite(poolStatusCacheExpiry,
                                          poolStatusCacheExpiryUnit)
                        .maximumSize(poolStatusCacheSize)
                        .softValues()
                        .build();
    }

    public void setNamespace(NameSpaceProvider namespace) {
        this.namespace = namespace;
    }

    public void setPnfsInfoCacheExpiry(int pnfsInfoCacheExpiry) {
        this.pnfsInfoCacheExpiry = pnfsInfoCacheExpiry;
    }

    public void setPnfsInfoCacheSize(int pnfsInfoCacheSize) {
        this.pnfsInfoCacheSize = pnfsInfoCacheSize;
    }

    public void setPnfsInfoCacheExpiryUnit(TimeUnit pnfsInfoCacheExpiryUnit) {
        this.pnfsInfoCacheExpiryUnit = pnfsInfoCacheExpiryUnit;
    }

    public void setPoolInfoCacheExpiry(int poolInfoCacheExpiry) {
        this.poolInfoCacheExpiry = poolInfoCacheExpiry;
    }

    public void setPoolInfoCacheSize(int poolInfoCacheSize) {
        this.poolInfoCacheSize = poolInfoCacheSize;
    }

    public void setPoolInfoCacheExpiryUnit(TimeUnit poolInfoCacheExpiryUnit) {
        this.poolInfoCacheExpiryUnit = poolInfoCacheExpiryUnit;
    }

    public void setPoolMonitor(PoolMonitor poolMonitor) {
        this.poolMonitor = poolMonitor;
    }

    public void setPoolStatusCacheExpiry(int poolStatusCacheExpiry) {
        this.poolStatusCacheExpiry = poolStatusCacheExpiry;
    }

    public void setPoolStatusCacheExpiryUnit(TimeUnit poolStatusCacheExpiryUnit) {
        this.poolStatusCacheExpiryUnit = poolStatusCacheExpiryUnit;
    }

    public void setPoolStatusCacheSize(int poolStatusCacheSize) {
        this.poolStatusCacheSize = poolStatusCacheSize;
    }
}
