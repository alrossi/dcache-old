package org.dcache.namespace.replication.data;

import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Set;

import diskCacheV111.namespace.NameSpaceProvider;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPoolGroup;
import diskCacheV111.poolManager.StorageUnit;
import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import org.dcache.auth.Subjects;
import org.dcache.namespace.FileAttribute;
import org.dcache.vehicles.FileAttributes;

/**
 * Encapsulates all the metadata concerning a given PnfsId/replica
 * needed by the replica manager, along with methods for refreshing
 * it against the namespace provider.
 * <p/>
 * The refresh on locations is provided so that a second check
 * can be made on a pnfsId-by-pnfsId basis after all copies of
 * that file have been pinned by the replica manager during an
 * operation eliminating redundant copies.
 *
 * Created by arossi on 1/29/15.
 */
public class PnfsIdInfo {
    private static final Set<FileAttribute> REQUIRED_ATTRIBUTES
                    = Collections.unmodifiableSet
                        (EnumSet.of(FileAttribute.ACCESS_LATENCY,
                                    FileAttribute.RETENTION_POLICY,
                                    FileAttribute.STORAGECLASS,
                                    FileAttribute.HSM,
                                    FileAttribute.LOCATIONS));

    public final PnfsId pnfsId;

    private final NameSpaceProvider nameSpaceProvider;

    private FileAttributes attributes;
    private Collection<String> locations;
    private StorageUnit storageUnit;

    /*
     * Constraints determined by pool group and storage unit.
     */
    private int maximum = 1;
    private int minimum = 1;
    private String onlyOneCopyPer;

    public PnfsIdInfo(PnfsId pnfsId, NameSpaceProvider nameSpaceProvider) {
        this.pnfsId = pnfsId;
        this.nameSpaceProvider = nameSpaceProvider;
    }

    public FileAttributes getAttributes() {
        return attributes;
    }

    public void getInfo(StringBuilder builder) {
        builder.append(pnfsId)
               .append("\n\t(sunit ").append(storageUnit)
               .append(")\n\t(min ").append(minimum)
               .append(")\n\t(min ").append(maximum)
               .append(")\n\t(1per ").append(onlyOneCopyPer)
               .append(")\n\t(online )").append(isOnline())
               .append(")\n\t(loc: ").append(locations)
               .append(")");
    }

    public Collection<String> getLocations() {
        return locations;
    }

    public Integer getMaximum() {
        return maximum;
    }

    public Integer getMinimum() {
        return minimum;
    }

    public synchronized int getNumberNeeded(Collection<String> active) {
        for (Iterator<String> it = locations.iterator(); it.hasNext(); ) {
            if (!active.contains(it.next())) {
                it.remove();
            }
        }

        int replicas = locations.size();
        int needed = minimum - replicas;

        if (needed > 0) {
            return needed;
        }

        needed = replicas - maximum;
        if (needed < 0) {
            return needed;
        }

        return 0;
    }

    public String getOnlyOneCopyPer() {
        return onlyOneCopyPer;
    }

    public StorageUnit getStorageUnit(PoolGroupInfo poolGroupInfo) {
        if (storageUnit == null) {
            String unitKey = attributes.getStorageClass();
            String hsm = attributes.getHsm();
            if (hsm != null) {
                unitKey += ("@" + hsm);
            }
            storageUnit =  poolGroupInfo.getStorageUnit(unitKey);
        }
        return storageUnit;
    }

    public boolean isOnline() {
        return attributes != null
                        && attributes.getAccessLatency() == AccessLatency.ONLINE;
    }

    public synchronized Collection<String> refreshLocations() throws CacheException {
        locations.clear();
        locations.addAll(nameSpaceProvider.getCacheLocation(Subjects.ROOT,
                                                            pnfsId));
        return locations;
    }

    public synchronized PnfsIdInfo setAttributes() throws CacheException {
        attributes = nameSpaceProvider.getFileAttributes(Subjects.ROOT,
                                                         pnfsId,
                                                         REQUIRED_ATTRIBUTES);
        locations = attributes.getLocations();
        return this;
    }

    /**
     * Determine replication contraints:  minimum/maximum copies required,
     * and whether replicas have special constraints limiting them to
     * one per category (such as host, rack, etc.).
     *
     * @param poolGroupInfo for resilient group having default constraints.
     */
    public PnfsIdInfo setConstraints(PoolGroupInfo poolGroupInfo) {
        SelectionPoolGroup poolGroup = poolGroupInfo.getPoolGroup();
        minimum = poolGroup.getMinReplicas();
        maximum = poolGroup.getMaxReplicas();
        onlyOneCopyPer = poolGroup.getOnlyOneCopyPer();

        StorageUnit sunit = getStorageUnit(poolGroupInfo);

        if (sunit != null) {
            Integer smin = sunit.getMinReplicas();
            Integer smax = sunit.getMaxReplicas();

            if (smin != null) {
                minimum = smin;
            }

            if (smax != null) {
                maximum = smax;
            }

            String oneCopyPer = sunit.getOnlyOneCopyPer();
            if (oneCopyPer != null) {
                onlyOneCopyPer = oneCopyPer;
            }
        }
        return this;
    }
}
