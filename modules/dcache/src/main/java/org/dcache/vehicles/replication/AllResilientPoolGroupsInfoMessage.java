package org.dcache.vehicles.replication;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import diskCacheV111.vehicles.Message;

import org.dcache.replication.v3.vehicles.ResilientPoolInfo;

/**
 * @author arossi
 */
public class AllResilientPoolGroupsInfoMessage extends Message {
    private static final long serialVersionUID = 1L;

    private final List<ResilientPoolInfo> allPools = new ArrayList<>();

    public void addPoolInfo(ResilientPoolInfo message) {
        allPools.add(message);
    }

    public Iterator<ResilientPoolInfo> pools() {
        return allPools.iterator();
    }
}
