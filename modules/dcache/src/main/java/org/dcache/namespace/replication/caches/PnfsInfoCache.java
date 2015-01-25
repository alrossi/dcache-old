/*
COPYRIGHT STATUS:
Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
software are sponsored by the U.S. Department of Energy under Contract No.
DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
non-exclusive, royalty-free license to publish or reproduce these documents
and software for U.S. Government purposes.  All documents and software
available from this server are protected under the U.S. and Foreign
Copyright Laws, and FNAL reserves all rights.

Distribution of the software available from this server is free of
charge subject to the user following the terms of the Fermitools
Software Legal Information.

Redistribution and/or modification of the software shall be accompanied
by the Fermitools Software Legal Information  (including the copyright
notice).

The user is asked to feed back problems, benefits, and/or suggestions
about the software to the Fermilab Software Providers.

Neither the name of Fermilab, the  URA, nor the names of the contributors
may be used to endorse or promote products derived from this software
without specific prior written permission.

DISCLAIMER OF LIABILITY (BSD):

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED  WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED  WARRANTIES OF MERCHANTABILITY AND FITNESS
FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL FERMILAB,
OR THE URA, OR THE U.S. DEPARTMENT of ENERGY, OR CONTRIBUTORS BE LIABLE
FOR  ANY  DIRECT, INDIRECT,  INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
OF SUBSTITUTE  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY  OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT  OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE  POSSIBILITY OF SUCH DAMAGE.

Liabilities of the Government:

This software is provided by URA, independent from its Prime Contract
with the U.S. Department of Energy. URA is acting independently from
the Government and in its own private capacity and is not acting on
behalf of the U.S. Government, nor as its contractor nor its agent.
Correspondingly, it is understood and agreed that the U.S. Government
has no connection to this software and in no manner whatsoever shall
be liable for nor assume any responsibility or obligation for any claim,
cost, or damages arising out of or resulting from the use of the software
available from this server.

Export Control:

All documents and software available from this server are subject to U.S.
export control laws.  Anyone downloading information from this server is
obligated to secure any necessary Government licenses before exporting
documents or software obtained from this server.
 */
package org.dcache.namespace.replication.caches;

import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import diskCacheV111.namespace.NameSpaceProvider;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import org.dcache.auth.Subjects;
import org.dcache.namespace.FileAttribute;
import org.dcache.vehicles.FileAttributes;

/**
 * Because replica handling may involve many calls to the namespace,
 * precautions must be taken to avoid DOS.  This component assumes that
 * the values asked for are reasonably stable within the limits defined
 * for each timeout.  All information needed by the replica manager
 * concerning file attributes should pass through
 * this cache.
 * <br>
 * This class is thread-safe.  It is assumed that access to the cache
 * will be on a dedicated thread.
 * <br>
 * @author arossi
 */
public class PnfsInfoCache extends
                AbstractResilientInfoCache<PnfsId, FileAttributes> {
    private static final Set<FileAttribute> requiredAttributes
                    = Collections.unmodifiableSet(
                    EnumSet.of(FileAttribute.ACCESS_LATENCY,
                                    FileAttribute.STORAGECLASS,
                                    FileAttribute.HSM));
    private NameSpaceProvider namespace;

    /**
     * This does not go through the cache, but is here for convenience.
     *
     * @param pnfsId of for which all locations should be returned.
     * @return locations (replicas) of file.
     * @throws CacheException
     */
    public List<String> getAllLocationsFor(PnfsId pnfsId) throws CacheException {
        return namespace.getCacheLocation(Subjects.ROOT, pnfsId);
    }

    /**
     * Calls cache.get().
     *
     * @param pnfsId of file for which to retrieve attributes.
     * @return ACCESS_LATENCY, STORAGECLASS and HSM name.
     * @throws ExecutionException
     */
    public FileAttributes getAttributes(PnfsId pnfsId)
                    throws ExecutionException {
        FileAttributes attributes = cache.get(pnfsId, ()-> load(pnfsId));
        if (attributes != null) {
            return attributes;
        }
        throw new NoSuchElementException(pnfsId.toString()
                        + " has no mapped attributes.");
    }

    /**
     * Depending on the size of the collection, this may
     * take considerable time, and thus should be handled on a thread
     * which will not block other operations.
     *
     * @param pnfsIds collection of files for which to load attributes into
     *                the cache.
     * @throws ExecutionException
     */
    public void loadAttributesFor(Collection<PnfsId> pnfsIds)
                    throws ExecutionException {
        for (PnfsId pnfsId: pnfsIds) {
            cache.get(pnfsId, ()-> load(pnfsId));
        }
    }

    public void setNamespace(NameSpaceProvider namespace) {
        this.namespace = namespace;
    }

    /*
     *  Single call to namespace provider.  Note that the storageclass
     *  attribute will go through the extractor which determines which
     *  attributes constitute storage class.
     */
    private FileAttributes load(PnfsId pnsfId) throws CacheException {
        return namespace.getFileAttributes(Subjects.ROOT,
                                           pnsfId,
                                           requiredAttributes);
    }
}
