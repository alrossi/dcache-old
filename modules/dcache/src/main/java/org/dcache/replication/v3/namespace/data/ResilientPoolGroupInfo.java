package org.dcache.replication.v3.namespace.data;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPoolGroup;
import diskCacheV111.poolManager.StorageUnit;

/**
 * @author arossi
 */
public class ResilientPoolGroupInfo {
    private final Map<String, StorageUnit> storageUnits;

    private SelectionPoolGroup poolGroup;

    public ResilientPoolGroupInfo() {
        storageUnits = new HashMap<>();
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
