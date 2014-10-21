package org.dcache.replication.v3.namespace;

import diskCacheV111.poolManager.PoolSelectionUnit;

import org.dcache.replication.v3.vehicles.ResilientPoolInfoForPnfsId;

/**
 * @author arossi
 *
 */
public interface PoolInfoForPnfsIdTaskHandler {

    void handleDone(ResilientPoolInfoForPnfsId info);

    PoolSelectionUnit getSelectionUnit();
}
