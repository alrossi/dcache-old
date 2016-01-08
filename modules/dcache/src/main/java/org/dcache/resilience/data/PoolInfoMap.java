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
package org.dcache.resilience.data;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import diskCacheV111.poolManager.CostModule;
import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPool;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPoolGroup;
import diskCacheV111.poolManager.StorageUnit;
import diskCacheV111.pools.CostCalculationV5;
import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.pools.PoolV2Mode;
import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.PoolManagerGetPoolMonitor;
import diskCacheV111.vehicles.PoolManagerPoolInformation;
import org.dcache.pool.migration.PoolSelectionStrategy;
import org.dcache.poolmanager.PoolInfo;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.resilience.util.CopyLocationExtractor;
import org.dcache.resilience.util.RemoveLocationExtractor;
import org.dcache.resilience.util.StorageUnitInfoExtractor;
import org.dcache.util.CacheExceptionFactory;
import org.dcache.util.CellStubFactory;
import org.dcache.util.ExceptionMessage;
import org.dcache.util.NonReindexableList;
import org.dcache.resilience.util.RandomSelectionStrategy;
import org.dcache.vehicles.FileAttributes;

/**
 * <p>Serves as the central locus of pool-related information.</p>
 *
 * <p>The internal data structures hold a list of pool names and pool groups
 * which will always assign a new index number to a new member even if
 * some of the current members happen to be deleted via a PoolSelectionUnit
 * operation.</p>
 *
 * <p>The reason for doing this is to be able to store most of the pool info
 * associated with a given pnfs or pool operation in progress as index
 * references rather than strings.</p>
 *
 * <p>The relational tables represented by multimaps of indices
 * capture pool and storage unit membership in pool groups.  There are
 * also three maps which define the resilience constraints for a given
 * storage unit, the tags for a pool, and the live pool manager information
 * for a pool.</p>
 *
 * <p>This class also provides methods for determining the resilient group
 * of a given pool, the storage groups connected to a given pool group,
 * whether a pool or pool group is resilient, whether a pool group can
 * satisfy the constraints defined by the storage units bound to it,
 * the members of a pool group which are currently readable,
 * and possible copy and remove targets for a particular set of locations.</p>
 *
 * <p>{@link #reload} empties and rebuilds all psu-related information,
 * removing stale pools and groups from the principal lists.</p>
 *
 * <p>This class benefits from read-write synchronization, since
 *    there will be many more reads of what is for the most part
 *    stable information (note that the periodically refreshed information
 *    is synchronized within the PoolInformation object itself.)</p>
 *
 * <p>Class is not marked final for stubbing/mocking purposes.</p>
 *
 * Created by arossi on 8/1/15.
 */
public class PoolInfoMap {
    private static final Logger LOGGER = LoggerFactory.getLogger(PoolInfoMap.class);

    private static StorageUnitConstraints getConstraints(Integer storageUnit,
                                                         Map<Integer, ResilienceMarker> constraints) {
        ResilienceMarker marker = constraints.get(storageUnit);

        if (!(marker instanceof StorageUnitConstraints)) {
            return null;
        }

        StorageUnitConstraints sconstraints = (StorageUnitConstraints) marker;
        short required = 1;
        String oneCopyPer = null;

        if (sconstraints != null) {
            required = sconstraints.getRequired();
            oneCopyPer = sconstraints.getOneCopyPer();
        } else {
            /*
             *  This can occur if a link pointing to the pool group of this pool
             *  has been changed or removed, or a storage unit removed from
             *  the unit group associated with it.  The policy here will
             *  be to consider these files non-existent for the purpose of
             *  replication.  By leaving required = 1 we guarantee that
             *  the entry remains "invisible" to replication handling
             *  and no action will be taken.
             */
        }

        return new StorageUnitConstraints(required, oneCopyPer);
    }

    private final List<String>                        pools              = new NonReindexableList<>();
    private final List<String>                        groups             = new NonReindexableList<>();
    private final Map<Integer, ResilienceMarker>      constraints        = new HashMap<>();
    private final Map<Integer, PoolInformation>       poolInfo           = new HashMap();
    private final Multimap<Integer, Integer>          poolGroupToPool    = HashMultimap.create();
    private final Multimap<Integer, Integer>          poolToPoolGroup    = HashMultimap.create();
    private final Multimap<Integer, Integer>          storageToPoolGroup = HashMultimap.create();
    private final Multimap<Integer, Integer>          poolToStorageGroup = HashMultimap.create();

    private final ReadWriteLock lock = new ReentrantReadWriteLock(true);
    private final Lock write = lock.writeLock();
    private final Lock read = lock.readLock();

    private CellStubFactory       cellStubFactory;
    private PoolSelectionStrategy poolSelectionStrategy;
    private PoolMonitor           monitor;

    private long     cacheExpiry     = 3;
    private TimeUnit cacheExpiryUnit = TimeUnit.MINUTES;
    private long     lastMonitorFetch;

    public void addStorageUnit(StorageUnit unit) {
        write.lock();
        try {
            String name = unit.getName();
            groups.add(name);
            setGroupConstraints(name, unit.getRequiredCopies(),
                            unit.getOnlyOneCopyPer());
        } finally {
            write.unlock();
        }
    }

    public void addStorageUnit(StorageUnit unit, String poolGroup) {
        write.lock();
        try {
            if (!groups.contains(unit.getName())) {
                addStorageUnit(unit);
            }

            Integer uIndex = groups.indexOf(unit.getName());
            addStorageUnitToGroup(unit.getName(), uIndex, poolGroup);
        } finally {
            write.unlock();
        }
    }

    public void addStorageUnit(StorageUnit unit,
                               Collection<String> poolGroups) {
        write.lock();
        try {
            if (!groups.contains(unit.getName())) {
                addStorageUnit(unit);
            }

            Integer uIndex = groups.indexOf(unit.getName());
            poolGroups.stream().forEach(
                            (group) -> addStorageUnitToGroup(unit.getName(),
                                            uIndex, group));
        } finally {
            write.unlock();
        }
    }

    public void addToPoolGroup(String pool, String group) {
        write.lock();
        try {
            Integer pindex = pools.indexOf(pool);
            Integer gindex = groups.indexOf(group);
            if (pindex != null && gindex != null) {
                poolGroupToPool.put(gindex, pindex);
                poolToPoolGroup.put(pindex, gindex);
            }
        } catch (NoSuchElementException e) {
            LOGGER.error("Could not add {} to {}: {}.",
                            pool, group, new ExceptionMessage(e));
        } finally {
            write.unlock();
        }
    }

    public StorageUnitConstraints getStorageUnitConstraints(Integer storageUnit) {
        return getConstraints(storageUnit, constraints);
    }

    public String getGroup(Integer group) {
        read.lock();
        try {
            return groups.get(group);
        } finally {
            read.unlock();
        }
    }

    public Integer getGroupIndex(String name) {
        read.lock();
        try {
            return groups.indexOf(name);
        } finally {
            read.unlock();
        }
    }

    public String getGroupName(Integer group) {
        read.lock();
        try {
            return groups.get(group);
        } finally {
            read.unlock();
        }
    }

    public String getPool(Integer pool) {
        read.lock();
        try {
            return pools.get(pool);
        } finally {
            read.unlock();
        }
    }

    public Integer getPoolIndex(String name) {
        read.lock();
        try {
            return pools.indexOf(name);
        } finally {
            read.unlock();
        }
    }

    public String getPoolInfo(String poolExpression) {
        final StringBuilder builder = new StringBuilder();
        final Pattern pattern;

        if (poolExpression != null) {
            pattern = Pattern.compile(poolExpression);
        } else {
            pattern = null;
        }

        Collection<Integer> matched;

        read.lock();
        try {
            matched = pools.stream()
                           .filter((p) -> pattern == null
                                           || pattern.matcher(p).find())
                           .map((p) -> pools.indexOf(p))
                           .collect(Collectors.toList());
        } finally {
            read.unlock();
        }

        matched.forEach((pool) -> {
            try {
                PoolInformation info = getPoolInformation(pool);
                if (info != null) {
                    builder.append(info).append("\n");
                }
            } catch (IllegalStateException e) {
                LOGGER.warn("Could not get info for {}: {}", pool,
                                new ExceptionMessage(e));
            }
        });

        return builder.toString();
    }

    public PoolManagerPoolInformation getPoolManagerInfo(
                    Integer pool)
                    throws ExecutionException, CacheException,
                    InterruptedException {
        PoolInformation info = getPoolInformation(pool);
        LOGGER.trace("getPoolManagerInfo {}, {}", pool, info);
        if (info == null) {
            throw new CacheException(String.format("Could not get pool manager "
                                                                   + "info for %s.",
                                                    getPool(pool)));
        }
        return info.getInfo(pools, cacheExpiryUnit.toMillis(cacheExpiry));
    }

    public PoolStateUpdate getPoolState(String pool) {
        return getPoolState(pool, null, null, null);
    }

    public PoolStateUpdate getPoolState(String pool, String storageUnit) {
        return getPoolState(pool, null, null, storageUnit);
    }

    public PoolStateUpdate getPoolState(String pool, Integer addedTo,
                                                     Integer removedFrom) {
        return getPoolState(pool, addedTo, removedFrom, null);
    }

    public PoolStateUpdate getPoolState(String pool, Integer addedTo,
                                                     Integer removedFrom,
                                                     String storageUnit) {
        PoolInformation info = getPoolInformation(getPoolIndex(pool));
        if (info != null) {
            return new PoolStateUpdate(pool, info.status, info.mode, addedTo,
                                       removedFrom, storageUnit);
        }

        /*
         * Cannot contact pool.  Treat it as DEAD.
         */
        return new PoolStateUpdate(pool, PoolStatusForResilience.DOWN,
                                   new PoolV2Mode(PoolV2Mode.DISABLED_DEAD),
                                   addedTo, removedFrom, storageUnit);
    }

    public PoolStatusForResilience getPoolStatus(String pool) {
        PoolInformation info = getPoolInformation(getPoolIndex(pool));
        if (info != null) {
            info.canWrite();
            return info.status;
        }
        return null;
    }

    public Collection<Integer> getPoolsOfGroup(Integer group) {
        read.lock();
        try {
            return poolGroupToPool.get(group);
        } finally {
            read.unlock();
        }
    }

    public Set<String> getReadableMemberLocations(Integer gindex,
                                                  Collection<String> locations) {
        Set<Integer> lindices;
        Collection<Integer> members;
        read.lock();
        try {
            lindices = locations.stream().map((l) -> pools.indexOf(l))
                                         .collect(Collectors.toSet());
            members = poolGroupToPool.get(gindex);
        } finally {
            read.unlock();
        }

        Set<Integer> mset = members.stream()
            .filter((i) -> {
                PoolInformation info = getPoolInformation(i);
                return info != null && info.canRead();
            }).collect(Collectors.toSet());

        lindices = Sets.intersection(lindices, mset);

        read.lock();
        try {
            return lindices.stream().map((i) -> pools.get(i))
                                    .collect(Collectors.toSet());
        } finally {
            read.unlock();
        }
    }

    public Integer getResilientPoolGroup(Integer pool) {
        read.lock();
        try {
            Set<Integer> rgroups = poolToPoolGroup.get(pool).stream()
                            .filter((g) -> constraints.get(g).isResilient())
                            .collect(Collectors.toSet());

            if (rgroups.size() == 0) {
                return null;
            }

            if (rgroups.size() > 1) {
                throw new IllegalStateException(String.format(
                                "Pool map is inconsistent; pool % belongs to "
                                                + "more than one resilient "
                                                + "group: %s.",
                                pools.get(pool),
                                rgroups.stream().map((i) -> groups.get(i))
                                                .collect(Collectors.toSet())));
            }

            return rgroups.iterator().next();
        } finally {
            read.unlock();
        }
    }

    public Set<String> getResilientPools() {
        read.lock();
        try {
            return pools.stream()
                        .filter((p) -> isResilientPool(p))
                        .collect(Collectors.toSet());
        } finally {
            read.unlock();
        }
    }

    public Collection<Integer> getPoolGroupsFor(String storageUnit) {
        Integer uindex = getGroupIndex(storageUnit);
        if (uindex == null) {
            return Collections.EMPTY_LIST;
        }

        read.lock();
        try {
            return storageToPoolGroup.get(uindex);
        } finally {
            read.unlock();
        }
    }

    public Collection<Integer> getStorageUnitsFor(String poolGroup) {
        Integer gindex = getGroupIndex(poolGroup);
        if (gindex == null) {
            return Collections.EMPTY_LIST;
        }

        read.lock();
        try {
            return poolToStorageGroup.get(gindex);
        } finally {
            read.unlock();
        }
    }

    public Integer getStorageUnitIndex(FileAttributes attributes) {
        String unitKey = attributes.getStorageClass();
        String hsm = attributes.getHsm();
        if (hsm != null) {
            unitKey += ("@" + hsm);
        }
        return getGroupIndex(unitKey);
    }

    public Map<String, String> getTags(Integer pool) {
        PoolInformation information = getPoolInformation(pool);
        if (information == null) {
            return Collections.EMPTY_MAP;
        }
        return information.tags;
    }

    public boolean isResilientGroup(Integer gindex) {
        read.lock();
        try {
            return constraints.get(gindex).isResilient();
        } finally {
            read.unlock();
        }
    }

    public boolean isResilientPool(String pool) {
        Integer pindex = getPoolIndex(pool);
        if (pindex != null) {
            return getResilientPoolGroup(pindex) != null;
        }
        return false;
    }

    /**
     * <p>Should be called on a dedicated thread.</p>
     *
     * <p>Reload will not clear the NonReindexable lists (pools, groups).
     * This is to maintain the same indices for the duration of the
     * life of the JVM, since the other maps may contain live references
     * to pools or groups.</p>
     *
     * @return list of pool indices for pools which should be rescanned.
     */
    public Collection<Integer> reload(PoolSelectionUnit psu) {
        write.lock();

        try {
            poolToPoolGroup.clear();
            storageToPoolGroup.clear();
            poolInfo.clear();

            Map<Integer, ResilienceMarker> constraints = new HashMap<>();
            constraints.putAll(this.constraints);
            this.constraints.clear();

            Multimap<Integer, Integer> poolGroupToPool = HashMultimap.create();
            poolGroupToPool.putAll(this.poolGroupToPool);
            this.poolGroupToPool.clear();

            Multimap<Integer, Integer> poolToStorageGroup = HashMultimap.create();
            poolToStorageGroup.putAll(this.poolToStorageGroup);
            this.poolToStorageGroup.clear();

            /*
             *  Pool removal assumes that the pool has been properly drained
             *  of files first; this is not taken care of here.  Similarly for
             *  the removal of pool groups.
             */
            removeStalePools(psu);
            removeStaleGroups(psu);

            psu.getSelectionUnits().values().stream()
                            .filter((u) -> u instanceof StorageUnit)
                            .map((u) -> (StorageUnit) u)
                            .forEach(this::addStorageUnit);

            psu.getPoolGroups().values().stream().forEach((p) ->
                addPoolGroup(p, StorageUnitInfoExtractor.getStorageUnitsInGroup(
                                p.getName(), psu)));

            psu.getPools().values().stream().forEach((p) ->
                addPool(p, p.getPoolGroupsMemberOf().stream()
                                .map((g) -> g.getName())
                                .collect(Collectors.toList())));

            Collection<Integer> poolsToRescan = new ArrayList<>();

            outer:
            for (SelectionPoolGroup group : psu.getPoolGroups().values()) {
                String name = group.getName();
                Integer gindex = getGroupIndex(name);
                Collection<Integer> currentPools = poolGroupToPool.get(gindex);
                ResilienceMarker previous = constraints.get(gindex);
                ResilienceMarker current = this.constraints.get(gindex);
                if (previous != null && !previous.equals(current)) {
                    poolsToRescan.addAll(currentPools);
                    continue outer;
                }

                if (poolToStorageGroup.get(gindex)
                                .equals(this.poolToStorageGroup.get(gindex))) {
                    poolsToRescan.addAll(currentPools);
                    continue outer;
                }

                Collection<Integer> sunits = getStorageUnitsFor(name);

                for (Integer sindex : sunits) {
                    previous = constraints.get(sindex);
                    current = this.constraints.get(sindex);
                    if (!current.equals(previous)) {
                        poolsToRescan.addAll(currentPools);
                        continue outer;
                    }
                }

                Collection<Integer> previousPools = poolGroupToPool.get(gindex);

                currentPools.stream().filter((i) -> !previousPools.contains(i))
                                     .forEach(poolsToRescan::add);
            }

            return poolsToRescan;
        } finally {
            write.unlock();
        }
    }

    public void removeFromPoolGroup(String pool, String group) {
        write.lock();
        try {
            Integer pindex = pools.indexOf(pool);
            Integer gindex = groups.indexOf(group);
            if (pindex != null && gindex != null) {
                poolGroupToPool.remove(gindex, pindex);
                poolToPoolGroup.remove(pindex, gindex);
            }
        } finally {
            write.unlock();
        }
    }

    public void removeGroup(String group) {
        write.lock();
        try {
            Integer index = groups.indexOf(group);
            groups.remove(index);
            constraints.remove(index);
            /*
             * One of these two sequences will be relevant, the other will
             * result in an empty set being returned.
             */
            poolGroupToPool.removeAll(index).stream()
                           .forEach((pindex) -> poolToPoolGroup.remove(pindex,
                                           index));
            poolToStorageGroup.removeAll(index).stream()
                           .forEach((gindex) -> storageToPoolGroup.remove(
                                           gindex, index));
            // OR
            storageToPoolGroup.removeAll(index).stream()
                           .forEach((gindex) -> poolToStorageGroup.remove(
                                           gindex, index));
        } finally {
            write.unlock();
        }
    }

    public void removePool(String pool) {
        write.lock();
        try {
            Integer pindex = getPoolIndex(pool);
            pools.remove(pindex);
            poolToPoolGroup.removeAll(pindex);
            poolInfo.remove(pindex);
        } finally {
            write.unlock();
        }
    }

    public void removeStorageUnit(String unit, Collection<String> poolGroups) {
        write.lock();
        try {
            Integer sgindex = groups.indexOf(unit);
            poolGroups.stream().forEach((group) -> removeStorageUnit(sgindex, group));
        } finally {
            write.unlock();
        }
    }

    public void removeStorageUnit(String unit, String group) {
        write.lock();
        try {
            Integer sgindex = groups.indexOf(unit);
            removeStorageUnit(sgindex, group);
        } finally {
            write.unlock();
        }
    }

    /**
     * <p>Filters first the writable pools in the pool group which do
     * not yet have a copy of this pnfsid, then applies any tag-induced
     * partitioning on the basis of the tags of copies that already exist.
     * From that list, it then chooses the pool using the
     * configured poolSelectionStrategy.</p>
     */
    public String selectCopyTarget(Integer gindex,
                                                Collection<String> locations,
                                                Set<Integer> tried,
                                                String oneCopyPer)
                    throws Exception {
        /*
         *  Writable locations in the pool group without a copy of this
         *  pnfsId.
         */
        Set<String> possible = getEligibleCopyTargets(gindex, locations, tried);

        /*
         * Filter by tag constraints.
         */
        CopyLocationExtractor extractor = new CopyLocationExtractor(oneCopyPer,
                                                                    this);
        locations.stream().forEach((l) -> extractor.addSeenTagsFor(l));
        Collection<String> candidates
                        = extractor.getCandidateLocations(possible);

        if (candidates.isEmpty()) {
            throw new Exception(String.format("Cannot satisfy copy request "
                                               + "because there are no (further) "
                                               + "possible locations; candidates %s",
                                              candidates));
        }

        /*
         *  Get pool cost for the candidates, then select the one using
         *  the inject poolSelectionStrategy.
         */
        List<PoolManagerPoolInformation> info = new ArrayList<>();
        for (String c : candidates) {
            /*
             * throws InterruptedException
             */
            info.add(getPoolManagerInfo(getPoolIndex(c)));
        }

        PoolManagerPoolInformation target = poolSelectionStrategy.select(info);

        LOGGER.debug("Pool selection poolSelectionStrategy "
                                     + "selected {} as copy target.", target);

        if (target == null) {
            throw new Exception(String.format("Cannot satisfy copy request "
                                               + "because the selection "
                                               + "algorithm returned "
                                               + "no viable locations; "
                                               + "locations: %s; possible %s",
                                              locations, candidates));
        }

        return target.getName();
    }

    /**
     * <p>Filters first the writable pools in the pool group which contain a copy
     * of this pnfsid, then applies any tag-induced partitioning on the basis
     * of the tags of copies that already exist to select a maximally
     * constrained pool.  If more than one pool has the same weight, one
     * of them is chosen randomly.</p>
     */
    public String selectRemoveTarget(Collection<String> locations,
                                                  String oneCopyPer)
                    throws Exception {
        Set<String> possible = getEligibleRemoveTargets(locations);
        RemoveLocationExtractor extractor
                        = new RemoveLocationExtractor(oneCopyPer, this);
        List<String> maximallyConstrained
                        = extractor.getCandidateLocations(possible);
        String target = RandomSelectionStrategy.SELECTOR.select(maximallyConstrained);

        if (target == null) {
            throw new Exception(String.format("Cannot satisfy remove request "
                                               + "because the selection algorithm returned "
                                               + "no viable locations: locations: %s; "
                                               + "possible: %s",
                                              locations,
                                              possible));
        }

        return target;
    }

    /**
     * <p>Chooses a source randomly from among the readable locations which have
     * not yet been tried.</p>
     */
    public String selectSource(Set<String> readable, Set<Integer> tried)
                    throws Exception {
        Set<String> excluded = tried.stream().map((p) -> getPool(p))
                                             .collect(Collectors.toSet());
        Set<String> possible = Sets.difference(readable, excluded);
        if (possible.isEmpty()) {
            throw new Exception(String.format("Cannot find a readable source "
                                               + "because there are no other "
                                               + "viable locations; "
                                               + "readable: %s; tried: %s",
                                              readable, excluded));
        }

        /*
         * A balancing selection strategy is not required here,
         * as we are choosing another source from which
         * to replicate, not an optimal target pool
         * (even if the source pool is "hot", this is a one-time
         * read).
         */
        return RandomSelectionStrategy.SELECTOR.select(possible);
    }

    public void setCacheExpiry(long cacheExpiry) {
        this.cacheExpiry = cacheExpiry;
    }

    public void setCacheExpiryUnit(TimeUnit cacheExpiryUnit) {
        this.cacheExpiryUnit = cacheExpiryUnit;
    }

    public void setGroupConstraints(String group, boolean resilient) {
        write.lock();
        try {
            constraints.put(groups.indexOf(group),
                            new ResilienceMarker(resilient));
        } finally {
            write.unlock();
        }
    }

    public void setGroupConstraints(String group, short required,
                                    String oneCopyPer) {
        write.lock();
        try {
            constraints.put(groups.indexOf(group),
                            new StorageUnitConstraints(required, oneCopyPer));
        } finally {
            write.unlock();
        }
    }

    public void setCellStubFactory(CellStubFactory cellStubFactory) {
        this.cellStubFactory = cellStubFactory;
    }

    public void setPoolSelectionStrategy(PoolSelectionStrategy strategy) {
        poolSelectionStrategy = strategy;
    }

    public void updatePoolStatus(PoolStateUpdate update) {
        Integer pindex = getPoolIndex(update.pool);
        PoolInformation poolInformation = getPoolInformation(pindex);
        if (poolInformation != null) {
            poolInformation.updateState(update);
        }
    }

    /**
     * A coarse-grained verification that the required and tag constraints
     * of the pool group and its associated storage groups can be met.
     * For the default and each storage unit, it attempts to fulfill the
     * max independent location requirement via the
     * {@link CopyLocationExtractor}.
     *
     * @throws IllegalStateException upon encountering the first set of
     *                               constraints which cannot be met.
     */
    public void verifyConstraints(Integer pgindex)
                    throws IllegalStateException {
        Collection<Integer> storageGroups;
        CopyLocationExtractor extractor;
        short required = 1;

        read.lock();
        try {
            storageGroups = poolToStorageGroup.get(pgindex);
        } finally {
            read.unlock();
        }

        for (Integer index : storageGroups) {
            read.lock();
            try {
                StorageUnitConstraints unitConstraints
                                = (StorageUnitConstraints) constraints.get(index);
                required = unitConstraints.getRequired();
                extractor = new CopyLocationExtractor(unitConstraints.getOneCopyPer(),
                                                      this);
            } finally {
                read.unlock();
            }

            verify(pgindex, extractor, required);
        }

    }

    @VisibleForTesting
    protected PoolMonitor getPoolMonitor() {
        /*
         * Throttle in case of successive calls.
         */
        if (monitor != null && System.currentTimeMillis() - lastMonitorFetch <
                        TimeUnit.SECONDS.toMillis(10)) {
            return monitor;
        }

        PoolManagerGetPoolMonitor msg = new PoolManagerGetPoolMonitor();

        write.lock();  // avoid multiple concurrent fetches
        try {
            msg = cellStubFactory.getPoolManager().sendAndWait(msg);
            if (msg == null) {
                throw CacheExceptionFactory.exceptionOf(CacheException.SERVICE_UNAVAILABLE,
                                "Could not fetch pool monitor.");
            }

            monitor = msg.getPoolMonitor();

            if (monitor == null) {
                throw CacheExceptionFactory.exceptionOf(CacheException.SERVICE_UNAVAILABLE,
                                "Could not fetch pool monitor.");
            }
        } catch (CacheException e) {
            LOGGER.error("{}.", new ExceptionMessage(e));
        } catch (InterruptedException e) {
            LOGGER.error("Fetch of pool information from pool monitor interrupted.");
        } finally {
            write.unlock();
        }

        lastMonitorFetch = System.currentTimeMillis();
        return monitor;
    }

    /*
     *  Write lock acquired by caller.
     */
    private void addPool(SelectionPool pool, Collection<String> poolGroups) {
        String name = pool.getName();
        pools.add(name);
        Integer pindex = pools.indexOf(name);
        poolGroups.forEach((group) -> {
            try {
                Integer gindex = groups.indexOf(group);
                poolGroupToPool.put(gindex, pindex);
                poolToPoolGroup.put(pindex, gindex);
            } catch (NoSuchElementException e) {
                LOGGER.error("{} belongs to pool group {} "
                                + "but the latter has not "
                                + "yet been mapped.", name, group);
            }
        });
    }

    /*
     *  Write lock acquired by caller.
     */
    private void addPoolGroup(SelectionPoolGroup group,
                              Collection<StorageUnit> units) {
        String name = group.getName();
        groups.add(name);
        setGroupConstraints(name, group.isResilient());

        Integer pgindex = groups.indexOf(name);
        units.stream().forEach((unit) -> {
            try {
                Integer sgindex = groups.indexOf(unit.getName());
                storageToPoolGroup.put(sgindex, pgindex);
                poolToStorageGroup.put(pgindex, sgindex);
            } catch (NoSuchElementException e) {
                LOGGER.error("{} is linked to pool group {} "
                                + "but has not "
                                + "yet been mapped.", unit, name);
            }
        });
    }

    /*
     *  Write lock acquired by caller.
     */
    private void addStorageUnitToGroup(String unit, Integer uIndex,
                                       String poolGroup) {
        try {
            Integer pgindex = groups.indexOf(poolGroup);
            storageToPoolGroup.put(uIndex, pgindex);
            poolToStorageGroup.put(pgindex, uIndex);
        } catch (NoSuchElementException e) {
            LOGGER.error("{} is linked to pool group {} "
                            + "but the latter has not "
                            + "yet been mapped.", unit, poolGroup);
        }
    }

    /**
     * <p>This method should be called only when the pool information object
     *    has not yet been initialized for a given pool.  This will occur
     *    only after a reload of the map or when a new pool is added;
     *    hence infrequently.</p>
     */
    private void fetchPoolInformationFromPoolMonitor() {
        LOGGER.trace("fetchPoolInformationFromPoolMonitor");
        PoolMonitor monitor = getPoolMonitor();

        if (monitor == null) {
            LOGGER.trace("fetchPoolInformationFromPoolMonitor failed: "
                                         + "could not retrieve monitor.");
            return;
        }

        CostModule costModule = monitor.getCostModule();

        write.lock();
        try {
            pools.stream()
                 .filter((p) -> !poolInfo.containsKey(pools.indexOf(p)))
                 .map((p) -> costModule.getPoolInfo(p))
                 .filter((i) -> i != null)
                 .forEach((i) -> setPoolInfo(i,
                                             costModule.getPoolCostInfo(i.getName())));
        } finally {
            write.unlock();
        }
    }

    private Set<String> getEligibleCopyTargets(Integer gindex,
                                               Collection<String> locations,
                                               Set<Integer> tried) {
        Set<Integer> lindices;
        Collection<Integer> members;
        read.lock();
        try {
            lindices = locations.stream()
                                .map((l) -> pools.indexOf(l))
                                .collect(Collectors.toSet());
            members = poolGroupToPool.get(gindex);
        } finally {
            read.unlock();
        }

        Set<Integer> mset = members.stream()
            .filter((i) -> {
                    PoolInformation info = getPoolInformation(i);
                    return info != null && info.canWrite();
                }).collect(Collectors.toSet());
        mset = Sets.difference(mset, lindices);
        mset = Sets.difference(mset, tried);

        read.lock();
        try {
            return mset.stream().map((i) -> pools.get(i))
                                .collect(Collectors.toSet());
        } finally {
            read.unlock();
        }
    }

    private Set<String> getEligibleRemoveTargets(Collection<String> locations) {
        Collection<Integer> lindices;

        read.lock();
        try {
            lindices = locations.stream().map((l) -> pools.indexOf(l))
                                         .collect(Collectors.toList());
        } finally {
            read.unlock();
        }

        for (Iterator<Integer> i = lindices.iterator(); i.hasNext(); ) {
            PoolInformation info = getPoolInformation(i.next());
            if (info == null || !info.canWrite()) {
                i.remove();
            }
        }

        read.lock();
        try {
            return lindices.stream().map((i) -> pools.get(i))
                                    .collect(Collectors.toSet());
        } finally {
            read.unlock();
        }
    }

    /*
     *  Initiates a pre-fetch of cost info and tags from pool monitor for
     *  all missing info if that info is available.
     */
    private PoolInformation getPoolInformation(Integer index) {
        PoolInformation information = null;
        read.lock();
        try {
            information = poolInfo.get(index);
        } finally {
            read.unlock();
        }

        if (information == null) {
            /*
             * Prefetch all missing info from the monitor.
             */
            fetchPoolInformationFromPoolMonitor();

            read.lock();
            try {
                information = poolInfo.get(index);
            } finally {
                read.unlock();
            }
        }

        return information;
    }

    /*
     * Write lock is acquired by caller.
     */
    private void removeStaleGroups(PoolSelectionUnit psu) {
        Set<String> allNew = psu.getSelectionUnits().values().stream()
                        .filter((u) -> u instanceof StorageUnit)
                        .map((u) -> u.getName())
                        .collect(Collectors.toSet());

        psu.getPoolGroups().values().stream()
                        .map((p) -> p.getName())
                        .forEach((n) -> allNew.add(n));

        groups.stream().filter((g) -> !allNew.contains(g))
                       .forEach((g) -> removeGroup(g));
    }

    /*
     * Write lock is acquired by caller.
     */
    private void removeStalePools(PoolSelectionUnit psu) {
        Set<String> allNew = psu.getPools().values().stream()
                        .map((p) -> p.getName())
                        .collect(Collectors.toSet());

        pools.stream().filter((p) -> !allNew.contains(p))
                      .forEach((p) -> removePool(p));
    }

    /*
     * Write lock is acquired by caller.
     */
    private void removeStorageUnit(Integer unit, String group) {
        Integer pgindex = groups.indexOf(group);
        if (pgindex == null) {
            LOGGER.debug("%s is linked to pool group {} but the latter has "
                                         + "not yet been mapped.", unit, group);
            return;
        }
        storageToPoolGroup.remove(unit, pgindex);
        poolToStorageGroup.remove(pgindex, unit);
    }

    /*
     * Write lock is acquired by caller.
     */
    private void setPoolInfo(PoolInfo info, PoolCostInfo cost) {
        String name = info.getName();
        Integer pindex = pools.indexOf(name);
        PoolInformation entry = new PoolInformation(pindex, name, cellStubFactory);
        entry.tags = info.getTags();
        CostCalculationV5 calc = new CostCalculationV5(cost);
        calc.recalculate();
        double pcost = calc.getPerformanceCost();
        entry.info = new PoolManagerPoolInformation(name, cost, pcost);
        entry.timestamp = System.currentTimeMillis();
        LOGGER.trace("setPoolInfo {} {}", name, entry);
        poolInfo.put(pindex, entry);
    }

    /**
     * @param index     of pool group or storage unit.
     * @param extractor configured for the specific tag constraints.
     * @param required  specific to this group or storage unit.
     * @throws IllegalStateException upon encountering the first set of
     *                               constraints which cannot be met.
     */
    private void verify(Integer index,
                        CopyLocationExtractor extractor,
                        int required) throws IllegalStateException {
        Set<String> members;

        read.lock();
        try {
            members = poolGroupToPool.get(index).stream()
                                     .map((i) -> pools.get(i))
                                     .collect(Collectors.toSet());
        } finally {
            read.unlock();
        }

        /*
         *  Note that the necessary read/write locking takes
         *  place on the internal extractor calls.
         */
        for (int i = 0; i < required; i++) {
            Collection<String> candidates
                            = extractor.getCandidateLocations(members);
            if (candidates.isEmpty()) {
                throw new IllegalStateException(getGroup(index));
            }
            String selected = RandomSelectionStrategy.SELECTOR.select(candidates);
            members.remove(selected);
            extractor.addSeenTagsFor(selected);
        }
    }
}