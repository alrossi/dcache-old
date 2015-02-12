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

import com.google.common.collect.ImmutableList;

import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;

import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.PoolManagerGetPoolsByPoolGroupMessage;
import org.dcache.cells.CellStub;
import org.dcache.namespace.replication.ReplicationHub;

/**
 * Because migration tasks may involve calls to the pool manager,
 * precautions are taken to avoid DOS on that service.  This cache assumes
 * that the values asked for are reasonably stable within the limits defined
 * for the timeout.  All information needed by the replication process
 * concerning the pool manager should pass through this cache.
 * <p/>
 *
 * This class is thread-safe.  It is assumed that access to the cache
 * will be on a dedicated thread.
 * <p/>
 *
 * This cache is principally the provider for the migration task's refreshable
 * list, offering caching instead of the asynchronous callback
 * which is implemented in {@link org.dcache.pool.migration.PoolListByPoolGroup).
 * It caches the message which contains the pool data as sent back
 * from the pool manager.
 *
 * Created by arossi on 1/22/15.
 */
public final class PoolManagerPoolInfoCache
                extends AbstractResilientInfoCache<String,
                                                   PoolManagerGetPoolsByPoolGroupMessage> {
    private CellStub poolManager;

    public PoolManagerGetPoolsByPoolGroupMessage getMessage(String poolGroup)
            throws ExecutionException {
        PoolManagerGetPoolsByPoolGroupMessage msg
                        = cache.get(poolGroup, ()->load(poolGroup));

        if (msg == null) {
            throw new NoSuchElementException(String.format("Pool Manager has "
                            + "no information about pool group %s.",
                            poolGroup));
        }

        return msg;
    }

    /*
     * Synchronous call to pool manager.
     */
    private PoolManagerGetPoolsByPoolGroupMessage load(String poolGroup)
                    throws CacheException, InterruptedException {
        PoolManagerGetPoolsByPoolGroupMessage msg =
            new PoolManagerGetPoolsByPoolGroupMessage(ImmutableList.of(poolGroup));
        msg = poolManager.sendAndWait(msg);
        Exception t = (Exception)msg.getErrorObject();
        if (t != null) {
            throw new CacheException(String.format("Failure to get data from "
                            + "pool manager for pool group %s: %s.",
                            poolGroup,
                            ReplicationHub.exceptionMessage(t)));
        }
        return msg;
    }

    public void setPoolManager(CellStub poolManager) {
        this.poolManager = poolManager;
    }

    @Override
    protected void prettyPrint(PoolManagerGetPoolsByPoolGroupMessage value,
                               StringBuilder builder) {
        builder.append(value);
    }
}
