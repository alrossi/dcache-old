package org.dcache.replication.v3.namespace.tasks;

import java.util.Collection;
import java.util.Iterator;

import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionLink;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPoolGroup;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionUnit;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionUnitGroup;
import diskCacheV111.poolManager.StorageUnit;

import org.dcache.replication.v3.namespace.PoolInfoForPnfsIdTaskHandler;
import org.dcache.replication.v3.vehicles.ResilientPoolInfo;


/**
 * @author arossi
 */
public class ResilientPoolInfoTask implements Runnable {

    private final ResilientPoolInfo info;
    private final PoolInfoForPnfsIdTaskHandler handler;

    private PoolSelectionUnit psu;

    public ResilientPoolInfoTask(ResilientPoolInfo info,
                                 PoolInfoForPnfsIdTaskHandler handler) {
        this.info = info;
        this.handler = handler;
    }

    public void run() {
        psu = handler.getSelectionUnit();

        info.setPoolGroup(getResilientPoolGroupOfPool());
        if (!info.isResilient()) {
            return;
        }

        getStorageUnitsInGroup();
    }

    private SelectionPoolGroup getResilientPoolGroupOfPool() {
        Collection<SelectionPoolGroup> pgroups
            = psu.getPoolGroupsOfPool(info.poolName);
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

    private void getStorageUnitsInGroup() {
        Collection<SelectionLink> links
            = psu.getLinksPointingToPoolGroup(info.getPoolGroup().getName());
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
}
