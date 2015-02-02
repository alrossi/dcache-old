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
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import org.dcache.namespace.replication.db.LocalNamespaceAccess;
import org.dcache.namespace.replication.data.PnfsIdInfo;

/**
 * Because replica handling may involve many calls to the namespace,
 * precautions must be taken to avoid DOS.  This component assumes that
 * the values asked for are reasonably stable within the limits defined
 * for each timeout.  All information needed by the replica manager
 * concerning metadata for a replica should be done via request to this cache,
 * eventually calling the appropriate refresh or extract methods
 * on the info object,
 * <p/>
 * This class is thread-safe.  It is assumed that access to the cache
 * will be on a dedicated thread.
 * <p/>
 *
 * Created by arossi on 1/22/15.
 */
public class PnfsInfoCache extends
                AbstractResilientInfoCache<PnfsId, PnfsIdInfo> {
    private LocalNamespaceAccess access;

    /**
     * Calls cache.get().
     *
     * @param pnfsId of file for which to retrieve metadata object.
     *               Note that this object will have valid attributes and
     *               locations set, but its replication constraints
     *               will not yet be determined because the pool group
     *               info needs to be passed to it.
     * @return object encapsulating necessary replica manager metadata.
     * @throws ExecutionException
     */
    public PnfsIdInfo getPnfsIdInfo(PnfsId pnfsId) throws ExecutionException {
        PnfsIdInfo info = cache.get(pnfsId, ()-> load(pnfsId));
        if (info != null) {
            return info;
        }
        throw new NoSuchElementException(pnfsId.toString()
                        + " has no associated metadata.");
    }

    /**
     * Depending on the size of the collection, this may
     * take considerable time, and thus should be handled on a thread
     * which will not block other operations.
     * <p/>
     * Just as with the single call, the attributes and locations for each
     * will be valid, but the constraints will not be indeterminate.
     *
     * @param location for which to load all pnfsid information, including
     *                 attributes and locations.
     * @param exclude these pnfsids if seen.
     * @return list of all pnfsids at location whose info has been cached.
     * @throws CacheException
     */
    public Collection<PnfsId> loadAll(String location, Collection<String> exclude)
                    throws CacheException {
        Collection<String> pnfsids = access.getAllPnfsidsFor(location);
        Map<PnfsId, PnfsIdInfo> infoMap = new HashMap<>();
        for (String pnfsid: pnfsids) {
            if (exclude.contains(pnfsid)) {
                continue;
            }
            PnfsId pnfsId = new PnfsId(pnfsid);
            PnfsIdInfo info = new PnfsIdInfo(pnfsId, access);
            info.setAttributes();
            infoMap.put(pnfsId, info);
        }
        cache.putAll(infoMap);
        return infoMap.keySet();
    }

    public void setAccess(LocalNamespaceAccess access) {
        this.access = access;
    }

    @Override
    protected void prettyPrint(PnfsIdInfo value, StringBuilder builder) {
        value.getInfo(builder);
    }

    private PnfsIdInfo load(PnfsId pnfsId) throws CacheException {
        PnfsIdInfo info = new PnfsIdInfo(pnfsId, access);
        info.setAttributes();
        return info;
    }
}
