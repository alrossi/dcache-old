package org.dcache.namespace.replication;

import diskCacheV111.namespace.NameSpaceProvider;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import org.dcache.auth.Subjects;
import org.dcache.namespace.FileAttribute;
import org.dcache.util.replication.AbstractResilientInfoCache;
import org.dcache.vehicles.FileAttributes;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Created by arossi on 1/22/15.
 */
public class PnfsInfoCache extends AbstractResilientInfoCache<PnfsId, FileAttributes> {
    private static final Set<FileAttribute> requiredAttributes
                    = Collections.unmodifiableSet(EnumSet.of(FileAttribute.ACCESS_LATENCY,
                                                             FileAttribute.STORAGECLASS,
                                                             FileAttribute.HSM));
    private NameSpaceProvider namespace;

    public List<String> getAllLocationsFor(PnfsId pnfsId) throws CacheException {
        return namespace.getCacheLocation(Subjects.ROOT, pnfsId);
    }

    public FileAttributes getAttributes(PnfsId pnfsId)
                    throws ExecutionException {
        FileAttributes attributes
                        = cache.get(pnfsId,
                                    ()-> namespace.getFileAttributes(Subjects.ROOT,
                                                                     pnfsId,
                                                                     requiredAttributes));
        if (attributes != null) {
            return attributes;
        }
        throw new NoSuchElementException(pnfsId.toString()
                        + " has no mapped attributes.");
    }

    public void setNamespace(NameSpaceProvider namespace) {
        this.namespace = namespace;
    }
}
