package org.dcache.replication.v3.namespace.tasks;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

import diskCacheV111.namespace.NameSpaceProvider;
import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;

import org.dcache.cells.CellStub;
import org.dcache.namespace.FileAttribute;
import org.dcache.replication.v3.namespace.ResilientInfoRegistry;
import org.dcache.replication.v3.vehicles.ResilientFileInfoMessage;
import org.dcache.vehicles.FileAttributes;

import static org.dcache.namespace.FileAttribute.ACCESS_LATENCY;
import static org.dcache.namespace.FileAttribute.HSM;
import static org.dcache.namespace.FileAttribute.STORAGECLASS;

/**
 * @author arossi
 */
public class ResilientFileInfoTask implements Runnable {
    private static final Logger LOGGER
        = LoggerFactory.getLogger(ResilientFileInfoTask.class);
    private static final Set<FileAttribute> requiredAttributes
        = Collections.unmodifiableSet(EnumSet.of(ACCESS_LATENCY,
                                                 STORAGECLASS,
                                                 HSM));
    private final PnfsId pnfsId;
    private final CellStub pool;
    private final Subject subject;
    private final NameSpaceProvider namespace;
    private final ResilientInfoRegistry registry;

    public ResilientFileInfoTask(PnfsId pnfsId,
                                 CellStub pool,
                                 Subject subject,
                                 ResilientInfoRegistry registry,
                                 NameSpaceProvider namespace) {
        this.pnfsId = pnfsId;
        this.pool = pool;
        this.subject = subject;
        this.registry = registry;
        this.namespace = namespace;
    }

    public void run() {
        FileAttributes attributes = null;
        try {
            attributes = namespace.getFileAttributes(subject,
                                                     pnfsId,
                                                     requiredAttributes);
        } catch (CacheException t) {
            LOGGER.error("There was a problem getting storage info for {}: {}.",
                            pnfsId,
                            t.getMessage());
            return;
        }

        if (!attributes.getAccessLatency().equals(AccessLatency.ONLINE)) {
            LOGGER.debug("{} is not ONLINE; ignoring ...", pnfsId);
            return;
        }
        String sourcePool = pool.getDestinationPath().getCellName();
        registry.register(pnfsId, sourcePool, attributes);

        pool.send(new ResilientFileInfoMessage(pnfsId));
        LOGGER.debug("Sent PnfsSystemStickyInfoMessage for {} to {}",  pnfsId,
                                                                       pool);
    }
}
