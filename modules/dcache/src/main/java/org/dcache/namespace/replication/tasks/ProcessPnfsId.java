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
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import org.dcache.cells.CellStub;
import org.dcache.namespace.replication.ReplicaManagerHub;
import org.dcache.namespace.replication.data.PnfsIdInfo;
import org.dcache.namespace.replication.data.PoolGroupInfo;
import org.dcache.namespace.replication.db.LocalNamespaceAccess;
import org.dcache.vehicles.replication.CacheEntryInfoMessage;

/**
 * Task responsible for controlling the replication of a single pnfsid.
 * <p/>
 * It beings by retrieving file attributes for the pnfsid from
 * the namespace.  If the pnfsid has a valid AccessLatency,
 * the attribute contraints and locations are then set, so
 * as to be passed to the copy subtask.  A message is also sent
 * to the source pool to make sure the copy there has a system sticky
 * record.
 * <p/>
 * A copy task is then launched.  This parent awaits the completion of
 * the subtask and, depending on whether the subtask succeeded or not, then
 * launches a "cleanup" remove subtask to prune the system of extra replicas.
 * While the latter case should not occur normally, the extra
 * task is here as a precaution, and will complete quickly if
 * there is no work to do.  This present task considers itself done only
 * upon completion of this second subtask.
 *
 * Created by arossi on 1/22/15.
 */
public class ProcessPnfsId extends ReplicaTask {

    private final LocalNamespaceAccess access;
    private final PnfsIdInfo pnfsIdInfo;
    private ReplicaTaskFuture<MakeCopies> copy;
    private ReplicaTaskFuture<RemoveExtras> remove;

    public ProcessPnfsId(ReplicaTaskInfo info, ReplicaManagerHub hub) {
        super(info, hub);
        access = hub.getAccess();
        pnfsIdInfo = new PnfsIdInfo(info.pnfsId);
    }

    @Override
    public boolean cancel() {
        if (isDone()) {
            return true;
        }

        super.cancel();

        if (copy != null) {
            copy.cancel(true);
        }

        if (remove != null) {
            remove.cancel(true);
        }

        return isCancelled();
    }

    @Override
    public ReplicaTaskFuture<ProcessPnfsId> launch() {
        runFuture = hub.getProcessPnfsIdExecutor().submit(this);
        return new ReplicaTaskFuture<>(this);
    }

    /*
     *  A few opportunities have been taken to slice
     *  the execution of this task with checks to see if it has been
     *  cancelled or otherwise terminated.
     */
    @Override
    public void run() {
        try {
            getPnfsIdInfo();
            ensureSourceIsSticky();
        } catch (CacheException | ExecutionException | InterruptedException e) {
            failed(e);
            return;
        }

        copy();

        /*
         * Signal completion of the entire replication process triggered
         * by a new cache location.
         */
        completedAll();
    }

    @Override
    protected void failed(Exception e) {
        LOGGER.error(GENERAL_FAILURE_MESSAGE, info.pool,
                        "process pnfsid", exceptionMessage(e));
        failedAll();
    }

    private void ensureSourceIsSticky()
                    throws ExecutionException, InterruptedException {
        CacheEntryInfoMessage message = new CacheEntryInfoMessage(info.pnfsId);
        CellStub pool = hub.getCellStubFactory().getPoolStub(info.pool);

        Future<CacheEntryInfoMessage> future = pool.send(message);
        LOGGER.trace("Sent CacheEntryInfoMessage {}.", message);

        if (isDone()) {
            return;
        }

        /*
         * Block until ready.
         */
        future.get();

        if (future.isCancelled()) {
            /*
             * The cancellation can occur if the access latency for the
             * file was for some reason not ONLINE.
             */
            LOGGER.warn("Attempt to get cache entry was cancelled: {}.",
                            message);
            completedAll();
        }
    }

    private void getPnfsIdInfo() throws CacheException, ExecutionException {
        pnfsIdInfo.setAttributes(access);

        if (!pnfsIdInfo.getAttributes()
                       .getAccessLatency().equals(AccessLatency.ONLINE)) {
            LOGGER.debug("AccessLatency of {} is not ONLINE; ignoring ...",
                            info.pnfsId);
            completedAll();
            return;
        }

        /*
         * Should usually be in cache at this point.
         */
        PoolGroupInfo poolGroupInfo = hub.getPoolInfoCache()
                                         .getPoolGroupInfo(info.pool);
        pnfsIdInfo.setConstraints(poolGroupInfo);

        /*
         * Note that we do not check constraints here because
         * the single pnfsId update is based on the Migration Task
         * and presupposes that task will "do the right thing" in terms
         * of which copies should be created and retained.
         */
    }

    private void copy() {
        try {
            PoolGroupInfo poolGroupInfo =
                            hub.getPoolInfoCache().getPoolGroupInfo(info.pool);

            if (isDone()) {
                return;
            }

            copy = new MakeCopies(info,
                                  poolGroupInfo.getPoolGroup().getName(),
                                  pnfsIdInfo,
                                  hub).launch();
        } catch (ExecutionException e) {
            failed(e);
            return;
        }

        MakeCopies result = null;

        while (!copy.isDone()) {
            try {
                result = copy.get(1, TimeUnit.MINUTES);
            } catch (InterruptedException | TimeoutException e) {
                LOGGER.trace("Future get() for MakeCopies {}: {}.",
                                pnfsIdInfo.pnfsId, exceptionMessage(e));
            }
        }

        if (result == null) {
            failed(new Exception(String.format("Copy task for %s was not "
                            + "returned by task future.", pnfsIdInfo.pnfsId)));
        } else {
            removeExtras(result);
        }
    }

    private void removeExtras(MakeCopies copies) {
        if (isDone()) {
            return;
        }

        remove = new RemoveExtras(info,
                                  pnfsIdInfo,
                                  copies.getConfirmed(),
                                  hub).launch();

        while (!remove.isDone()) {
            try {
                remove.get(1, TimeUnit.MINUTES);
            } catch (InterruptedException | TimeoutException e) {
                LOGGER.trace("Future get() for RemoveExtras {}.",
                                pnfsIdInfo.pnfsId, exceptionMessage(e));
            }
        }
    }
}
