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
package org.dcache.replication.v3.namespace.tasks;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

import org.dcache.pool.migration.Task;
import org.dcache.pool.migration.TaskCompletionHandler;
import org.dcache.replication.v3.namespace.ReplicaManagerHub;
import org.dcache.replication.v3.namespace.ResilientInfoCache;
import org.dcache.replication.v3.namespace.data.PoolGroupInfo;
import org.dcache.replication.v3.namespace.data.ReplicaJobDefinition;
import org.dcache.replication.v3.vehicles.CacheEntryInfoMessage;
import org.dcache.vehicles.FileAttributes;

/**
 * A wrapper around the actual migration {@link Task}; runs
 * on a separate thread because it makes two calls to the cache
 * which may need to reload from either the pool manager or
 * the database.  The final run() call on the migration task
 * is executed on a different (scheduled) queue/service.
 *
 * @author arossi
 */
public class ReplicationTask implements Runnable {
    protected static final Logger LOGGER
        = LoggerFactory.getLogger(ReplicationTask.class);

    private final CacheEntryInfoMessage message;
    private final TaskCompletionHandler handler;
    private final ReplicaManagerHub hub;
    private final boolean useGreedyRequests;

    public ReplicationTask(CacheEntryInfoMessage message,
                           TaskCompletionHandler handler,
                           ReplicaManagerHub hub,
                           boolean useGreedyRequests) {
        this.message = message;
        this.handler = handler;
        this.hub = hub;
        this.useGreedyRequests = useGreedyRequests;
    }

    public void run() {
        String pool = message.getPool();

        FileAttributes attributes;
        PoolGroupInfo poolInfo;

        try {
            ResilientInfoCache cache = hub.getCache();
            attributes = cache.getAttributes(message.pnfsId);
            poolInfo = cache.getPoolGroupInfo(pool);
        } catch (ExecutionException t) {
            LOGGER.error("Fatal error prior to launching actual "
                            + "migration task for {}@{}: {}.",
                            message.pnfsId,
                            pool,
                            t.getMessage());
            return;
        }

        ReplicaJobDefinition jobDefinition
            = ReplicaJobDefinition.create(poolInfo,
                                          attributes,
                                          pool,
                                          useGreedyRequests,
                                          hub.getPoolManager());
        new Task(handler,
                 null,
                 hub.getPnfsManager(),
                 hub.getPinManager(),
                 hub.getMigrationTaskExecutor(),
                 pool,
                 message.getEntry(),
                 jobDefinition).run();
    }
}
