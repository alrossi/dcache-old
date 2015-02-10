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
package org.dcache.namespace.replication.tasks;

import java.util.concurrent.ExecutionException;

import org.dcache.namespace.replication.ReplicaManagerHub;
import org.dcache.namespace.replication.data.PoolGroupInfo;

/**
 * A preliminary task run in advance of the others.  It
 * checks the pool group info to see that it corresponds
 * to a resilient pool.  If it does, it spawns the next
 * task according to the type of operation.  Else
 * it sets the entire operation to its completed state.
 * </p>
 * Note that access to the pool group info includes populating
 * the info object with its set of pools, min/max replication
 * constraints, and its set of storage groups.  The object
 * is read from a cache which is in turn loaded from a remote
 * pool monitor provider.
 *
 * Created by arossi on 2/7/15.
 */
public final class VerifyPool extends ReplicaTask {

    public VerifyPool(ReplicaTaskInfo info, ReplicaManagerHub hub) {
        super(info, hub);
    }

    @Override
    public ReplicaTaskFuture<VerifyPool> launch() {
        runFuture = hub.getVerifyPoolExecutor().submit(this);
        return new ReplicaTaskFuture<>(this);
    }

    @Override
    public void run() {
        try {
            /*
             * Will refresh the information by reloading when the cache entry
             * does not exist or is stale.  The object contains all the
             * information needed to process pool constraints on replication,
             * and will be accessed again in the successive phase.
             */
            PoolGroupInfo poolGroupInfo
                             = hub.getPoolInfoCache().getPoolGroupInfo(info.pool);
            if (!poolGroupInfo.isResilient()) {
                LOGGER.debug("{} does not belong to a resilient group",
                                info.pool);
                completedAll();
            } else {
                nextTask();
            }
        } catch (ExecutionException | IllegalArgumentException t) {
            failed(t);
        }
    }

    @Override
    protected void failed(Exception e) {
        LOGGER.error(GENERAL_FAILURE_MESSAGE,
                        info.pool,
                        "verification of pool information",
                        exceptionMessage(e));

        failedAll();
    }

    private void nextTask() {
        switch (info.type) {
            case SINGLE:
                /*
                 * Set the future of the next task on the info object.
                 */
                info.setTaskFuture(new ProcessPnfsId(info, hub).launch());
                break;
            case POOL_SCAN:
                /*
                 * Start also sets the sentinel on the info object.
                 */
                new PoolScanSentinel(info, hub).start();
                break;
            case POOL_DOWN:
            case POOL_RESTART:
                /*
                 * Start also sets the sentinel on the info object.
                 */
                new PoolUpdateSentinel(info, hub).start();
                break;
            default:
                throw new IllegalArgumentException("Unrecognized type "
                                + info.type);
        }

        completed();
    }
}
