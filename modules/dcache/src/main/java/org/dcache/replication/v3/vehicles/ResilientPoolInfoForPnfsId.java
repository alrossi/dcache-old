package org.dcache.replication.v3.vehicles;

import javax.security.auth.Subject;

import diskCacheV111.util.PnfsId;

/**
 * @author arossi
 */
public final class ResilientPoolInfoForPnfsId extends ResilientPoolInfo {
    public final PnfsId pnfsId;

    public ResilientPoolInfoForPnfsId(PnfsId pnfsId, String poolName,
                    Subject subject) {
        super(poolName, subject);
        this.pnfsId = pnfsId;
    }
}
