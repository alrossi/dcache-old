package org.dcache.vehicles.replication;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import diskCacheV111.vehicles.Message;

import org.dcache.replication.v3.namespace.data.PoolGroupInfo;

/**
 * @author arossi
 */
public class AllResilientPoolGroupsInfoMessage extends Message {
    private static final long serialVersionUID = 1L;

    private final List<PoolGroupInfo> allPools = new ArrayList<>();

    public void addPoolInfo(PoolGroupInfo message) {
        allPools.add(message);
    }

    public Iterator<PoolGroupInfo> pools() {
        return allPools.iterator();
    }
}
