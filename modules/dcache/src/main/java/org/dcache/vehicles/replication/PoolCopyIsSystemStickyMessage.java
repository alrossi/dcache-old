package org.dcache.vehicles.replication;

import diskCacheV111.vehicles.PoolMessage;

import org.dcache.pool.repository.EntryState;

/**
 * Specifically asks only for system sticky bit.
 *
 * @author arossi
 */
public class PoolCopyIsSystemStickyMessage extends PoolMessage {
    private static final long serialVersionUID = -474283254721659633L;

    public final String pnfsid;

    private EntryState entryState;
    private boolean isSystemSticky = false;

    public PoolCopyIsSystemStickyMessage(String poolName, String pnfsid) {
        super(poolName);
        this.pnfsid = pnfsid;
    }

    public boolean isSystemSticky() {
        return isSystemSticky;
    }

    public void setSystemSticky(boolean isSystemSticky) {
        this.isSystemSticky = isSystemSticky;
    }

    public EntryState getEntryState() {
        return entryState;
    }

    public void setEntryState(EntryState entryState) {
        this.entryState = entryState;
    }
}
