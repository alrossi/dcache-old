package org.dcache.replication.v3.vehicles;

import org.python.google.common.base.Preconditions;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.Message;

import org.dcache.pool.repository.CacheEntry;

/**
 * @author arossi
 *
 */
public class CacheEntryInfoMessage extends Message {
    private static final long serialVersionUID = 1L;

    public final PnfsId pnfsId;

    private String pool;
    private CacheEntry entry;

    public CacheEntryInfoMessage(PnfsId pnfsId) {
        this.pnfsId = Preconditions.checkNotNull(pnfsId, "message lacks pnfsid");
    }

    public CacheEntry getEntry() {
        return entry;
    }

    public String getPool() {
        return pool;
    }

    public void setEntry(CacheEntry entry) {
        this.entry = entry;
    }

    public void setPool(String pool) {
        this.pool = pool;
    }
}
