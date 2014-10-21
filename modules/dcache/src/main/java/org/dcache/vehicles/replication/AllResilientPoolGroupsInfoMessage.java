package org.dcache.vehicles.replication;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import diskCacheV111.vehicles.Message;

import org.dcache.replication.v3.namespace.data.ResilientPoolGroupInfo;

/**
 * @author arossi
 */
public class AllResilientPoolGroupsInfoMessage extends Message {
    private static final long serialVersionUID = 1L;

    private final List<ResilientPoolGroupInfo> allPools = new ArrayList<>();

    public void addPoolInfo(ResilientPoolGroupInfo message) {
        allPools.add(message);
    }

    public Iterator<ResilientPoolGroupInfo> pools() {
        return allPools.iterator();
    }
}
