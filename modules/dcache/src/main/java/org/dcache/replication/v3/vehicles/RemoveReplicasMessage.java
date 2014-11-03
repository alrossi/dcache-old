package org.dcache.replication.v3.vehicles;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.Message;

/**
 * @author arossi
 *
 */
public class RemoveReplicasMessage extends Message {
    private static final long serialVersionUID = 1L;
    private final Collection<PnfsId> toRemove = new ArrayList<>();

    public RemoveReplicasMessage(Collection<PnfsId> toRemove) {
        this.toRemove.addAll(toRemove);
    }

    public Iterator<PnfsId> iterator() {
        return toRemove.iterator();
    }
}
