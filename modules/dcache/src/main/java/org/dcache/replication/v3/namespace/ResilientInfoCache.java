package org.dcache.replication.v3.namespace;

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
import org.dcache.replication.v3.namespace.data.ResilientPoolGroupInfo;
import org.dcache.vehicles.FileAttributes;

import static org.dcache.namespace.FileAttribute.*;

/**
 * @author arossi
 *
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

    private class PoolInfoFetcher extends CacheLoader<String, ResilientPoolGroupInfo> {

        @Override
        public ResilientPoolGroupInfo load(String pool) throws Exception {
            ResilientPoolGroupInfo info = new ResilientPoolGroupInfo();
            getResilientPoolGroupOfPool(pool, info);
            if (info.isResilient()) {
                getStorageUnitsInGroup(info);
            }
            loadAllPoolsInGroup(pool, info);
            return info;
        }

        private void getResilientPoolGroupOfPool(String pool,
                                                 ResilientPoolGroupInfo info) {
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

        private void getStorageUnitsInGroup(ResilientPoolGroupInfo info) {
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

        private void loadAllPoolsInGroup(String source, ResilientPoolGroupInfo info) {
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
    private int pnfsInfoCacheSize = 1000;
    private int poolInfoCacheSize = 1000;
    private TimeUnit pnfsInfoCacheTimeoutUnit = TimeUnit.MINUTES;
    private TimeUnit poolInfoCacheTimeoutUnit = TimeUnit.MINUTES;

    /*
     * From initialization.
     */
    private LoadingCache<PnfsId, FileAttributes> pnfsInfoCache;
    private LoadingCache<String, ResilientPoolGroupInfo> poolInfoCache;

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

    public ResilientPoolGroupInfo getPoolGroupInfo(String pool)
                    throws ExecutionException {
        ResilientPoolGroupInfo info = poolInfoCache.get(pool);
        if (info != null) {
            return info;
        }
        throw new NoSuchElementException(pool
                        + " has no mapped information for pool group.");
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
                        .expireAfterAccess(pnfsInfoCacheExpiry,
                                           pnfsInfoCacheTimeoutUnit)
                        .maximumSize(pnfsInfoCacheSize)
                        .softValues()
                        .build(new PnfsInfoFetcher());

        poolInfoCache = CacheBuilder.newBuilder()
                        .expireAfterAccess(poolInfoCacheExpiry,
                                           poolInfoCacheTimeoutUnit)
                        .maximumSize(poolInfoCacheSize)
                        .softValues()
                        .build(new PoolInfoFetcher());
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

    public void setPnfsInfoCacheTimeoutUnit(TimeUnit pnfsInfoCacheTimeoutUnit) {
        this.pnfsInfoCacheTimeoutUnit = pnfsInfoCacheTimeoutUnit;
    }

    public void setPoolInfoCacheExpiry(int poolInfoCacheExpiry) {
        this.poolInfoCacheExpiry = poolInfoCacheExpiry;
    }

    public void setPoolInfoCacheSize(int poolInfoCacheSize) {
        this.poolInfoCacheSize = poolInfoCacheSize;
    }

    public void setPoolInfoCacheTimeoutUnit(TimeUnit poolInfoCacheTimeoutUnit) {
        this.poolInfoCacheTimeoutUnit = poolInfoCacheTimeoutUnit;
    }

    public void setPoolMonitor(PoolMonitor poolMonitor) {
        this.poolMonitor = poolMonitor;
    }
}
