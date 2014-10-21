package org.dcache.replication.v3.vehicles;

import javax.security.auth.Subject;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPoolGroup;
import diskCacheV111.poolManager.StorageUnit;

/**
 * @author arossi
 */
public class ResilientPoolInfo {
    public final String poolName;
    public final Subject subject;

    private final Map<String, StorageUnit> storageUnits;
    private SelectionPoolGroup poolGroup;

    public ResilientPoolInfo(String poolName, Subject subject) {
        this.poolName = poolName;
        this.subject = subject;
        storageUnits = new HashMap<>();
    }

    public ResilientPoolInfo(ResilientPoolInfo info) {
        poolName = info.poolName;
        subject = info.subject;
        storageUnits = info.storageUnits;
        poolGroup = info.poolGroup;
    }

    public void addStorageUnit(StorageUnit storageUnit) {
        storageUnits.put(storageUnit.getName(), storageUnit);
    }

    public SelectionPoolGroup getPoolGroup() {
        return poolGroup;
    }

    public StorageUnit getStorageUnit(String unitName) {
        return storageUnits.get(unitName);
    }

    public boolean isResilient() {
        return poolGroup != null;
    }

    public void setPoolGroup(SelectionPoolGroup poolGroup) {
        this.poolGroup = poolGroup;
    }

    public Iterator<StorageUnit> storageUnits() {
        return storageUnits.values().iterator();
    }
}
