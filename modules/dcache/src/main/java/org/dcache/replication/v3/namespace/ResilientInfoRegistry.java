package org.dcache.replication.v3.namespace;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import diskCacheV111.util.PnfsId;

import org.dcache.replication.v3.vehicles.ResilientPoolInfo;
import org.dcache.vehicles.FileAttributes;

/**
 * @author arossi
 *
 */
public class ResilientInfoRegistry {
    private static final Logger LOGGER
        = LoggerFactory.getLogger(ResilientInfoRegistry.class);

    private class ResilientPnfsInfo {
        String sourcePool;
        FileAttributes attributes;
    }

    private final Map<String, ResilientPnfsInfo> pnfsIdMap = new HashMap<>();
    private final Map<String, ResilientPoolInfo> poolInfoMap = new HashMap<>();

    public synchronized FileAttributes getAttributes(PnfsId pnfsId) {
        ResilientPnfsInfo info = pnfsIdMap.get(pnfsId.toString());
        if (info != null) {
            if (info.attributes == null) {
                throw new IllegalStateException(pnfsId.toString()
                                + " has no attributes mapped; this is a bug.");
            }
            return info.attributes;
        }
        throw new NoSuchElementException(pnfsId.toString());
    }

    public synchronized ResilientPoolInfo getPoolInfo(PnfsId pnfsId) {
        ResilientPnfsInfo info = pnfsIdMap.get(pnfsId.toString());
        if (info != null) {
            if (info.sourcePool == null) {
                throw new IllegalStateException(pnfsId.toString()
                                + " has no sourcePool mapped; this is a bug.");
            }
            return getPoolInfo(info.sourcePool);
        }
        throw new NoSuchElementException(pnfsId.toString());
    }

    public synchronized ResilientPoolInfo getPoolInfo(String pool) {
        ResilientPoolInfo info = poolInfoMap.get(pool);
        if (info != null) {
            return info;
        }
        throw new NoSuchElementException(pool);
    }

    public synchronized void register(PnfsId pnfsId,
                                      String sourcePool,
                                      FileAttributes attributes) {
        ResilientPnfsInfo info = new ResilientPnfsInfo();
        info.attributes = attributes;
        info.sourcePool = sourcePool;
        pnfsIdMap.put(pnfsId.toString(), info);
    }

    public synchronized void register(String pool, ResilientPoolInfo info) {
        poolInfoMap.put(pool, info);
    }

    public synchronized void remove(PnfsId pnfsId) {
        pnfsIdMap.remove(pnfsId.toString());
        LOGGER.debug("Removed {}.", pnfsId);
    }

    public synchronized void remove(String pool) {
        poolInfoMap.remove(pool);
        LOGGER.debug("Removed {}.", pool);
    }
}
