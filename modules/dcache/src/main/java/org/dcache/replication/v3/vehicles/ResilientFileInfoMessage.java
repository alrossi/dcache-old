package org.dcache.replication.v3.vehicles;

import org.python.google.common.base.Preconditions;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.Message;

/**
 * @author arossi
 *
 */
public class ResilientFileInfoMessage extends Message {
    private static final long serialVersionUID = 1L;

    public final PnfsId pnfsId;
    private boolean isSystemSticky;

    public ResilientFileInfoMessage(PnfsId pnfsId) {
        this.pnfsId = Preconditions.checkNotNull(pnfsId, "message lacks pnfsid");
        isSystemSticky = false;
    }

    public boolean isSystemSticky() {
        return isSystemSticky;
    }

    public void setSystemSticky(boolean isSystemSticky) {
        this.isSystemSticky = isSystemSticky;
    }
}
