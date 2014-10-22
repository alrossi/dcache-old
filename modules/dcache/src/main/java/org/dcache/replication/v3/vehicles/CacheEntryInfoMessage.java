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
    private boolean isSystemSticky;

    public CacheEntryInfoMessage(PnfsId pnfsId) {
        this.pnfsId = Preconditions.checkNotNull(pnfsId, "message lacks pnfsid");
        isSystemSticky = false;
    }

    public CacheEntry getEntry() {
        return entry;
    }

    public String getPool() {
        return pool;
    }

    public boolean isSystemSticky() {
        return isSystemSticky;
    }

    public void setEntry(CacheEntry entry) {
        this.entry = entry;
    }

    public void setPool(String pool) {
        this.pool = pool;
    }

    public void setSystemSticky(boolean isSystemSticky) {
        this.isSystemSticky = isSystemSticky;
    }
}
