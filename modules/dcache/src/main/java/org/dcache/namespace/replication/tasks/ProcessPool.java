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

import java.text.ParseException;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.RetentionPolicy;
import org.dcache.alarms.AlarmMarkerFactory;
import org.dcache.alarms.PredefinedAlarm;
import org.dcache.namespace.replication.ReplicationHub;
import org.dcache.namespace.replication.data.PnfsIdInfo;
import org.dcache.namespace.replication.data.PoolGroupInfo;
import org.dcache.namespace.replication.db.LocalNamespaceAccess;
import org.dcache.namespace.replication.db.PnfsInfoQuery;

/**
 * A task responsible for the handling of pool status changes
 * which may require either migration or replica reduction.
 * <p/>
 * The task is launched by the associated sentinel, either
 * {@link org.dcache.namespace.replication.tasks.PoolScanSentinel} or
 * {@link org.dcache.namespace.replication.tasks.PoolUpdateSentinel}.
 * If other status messages for this pool are received during the
 * intervening period, the update sentinel takes appropriate action by either
 * ignoring the message or restarting the wait with a new mode/type.
 * It is also responsible for handling messages arriving while the task
 * is actually running.  Depending on which messages arrive, the sentinal may
 * start another task like this one after the current task completes.
 * The scan sentinel merely discards incoming status messages for this pool.
 * The sentinel is unregistered when the worker completes.
 * </p>
 * The first thing the task does is to execute a query on the namespace which
 * scans the pnfsids for the pool and inspects each for its replicas, examining
 * the constraints that should be enforced.  For each one requiring action
 * (either to send an alarm that no source for the file is accessible, to make
 * extra copies for the files requiring them, or to remove extra copies),
 * the namespace method calls back to this task, initiating a subtask if
 * appropriate.
 * <p/>
 * The task then waits for all subtasks to complete.
 * <p/>
 * While each pnfsid's constraints are determined from both pool group and
 * storage group, excluding inactive pools, an attempt is made to limit the
 * initial result set from the query:  in the case of DOWN, this would be based
 * on an upper bound on the minimum replicas for the pool group; likewise,
 * for RESTART, the limit is based on the lower bound on the maximum.
 * For a scan operation, however, all pnfsids are returned.  It should be
 * noted that, since pnfsids are processed in sequence, copy or remove
 * actions will be taken only once on each, though they may be returned
 * multiple times from the namespace as the scan moves from pool to pool.
 *
 * Created by arossi on 2/7/15.
 */
public final class ProcessPool extends ReplicaTask implements PnfsIdProcessor {
    private static final String INACCESSIBLE_FILE_MESSAGE
                    = "Resilient pool {} is DOWN; it contains the only copy of "
                    + "the following files which should have a replica elsewhere. "
                    + "Administrator intervention is required.\n\n{}";

    /*
     *  These will be either copies or removes (or both), depending on
     *  the type of pool processing.
     */
    private final Queue<Future<? extends ReplicaTask>> taskFutures = new LinkedList<>();

    /*
     *  The inaccessible pnfsids to include in the alarm message.
     */
    private final Collection<PnfsId> singletons = new HashSet<>();

    private final LocalNamespaceAccess access;

    private PoolGroupInfo poolGroupInfo;
    private PnfsInfoQuery query;

    public ProcessPool(ReplicaTaskInfo info, ReplicationHub hub) {
        super(info, hub);
        access = hub.getAccess();
    }

    @Override
    public synchronized boolean cancel() {
        if (isDone()) {
            return true;
        }

        super.cancel();

        /*
         *  take care of cancelling the tasks that are running
         */
        while (!taskFutures.isEmpty()) {
            Future future = taskFutures.remove();
            future.cancel(true);
        }

        return isCancelled();
    }

    /*
     *  A few opportunities have been taken to slice
     *  the execution of this task with checks to see if it has been
     *  cancelled or otherwise terminated.
     */
    @Override
    public void run() {
        try {
            poolGroupInfo = hub.getPoolInfoCache().getPoolGroupInfo(info.pool);
            processPnfsIds();
            sendAlarms();
            int failed = waitForFutures();
            if (failed > 0) {
                failed(new Exception(String.format("%s task(s) failed to complete "
                                + "successfully.", failed)));
            } else {
                /*
                 * Signal completion of the entire replication process triggered
                 * by a pool message or scan action.
                 */
                completedAll();
            }
        } catch (ExecutionException e) {
            failed(e);
        }
    }

    @Override
    public ReplicaTaskFuture<ProcessPool> launch() {
        runFuture = hub.getProcessPoolExecutor().submit(this);
        return new ReplicaTaskFuture<>(this);
    }

    /*
     *  Adds the pnfsid to an alarm list to be processed when
     *  the query ends.
     */
    @Override
    public void processAlarm(PnfsIdInfo pnfsIdInfo)
                    throws InterruptedException {
        if (isDone()) {
            /*
             *  This will make sure the database access method
             *  which called back here is also interrupted.
             */
            throw new InterruptedException(String.format("%s, type %s, "
                            + "stopped.", info.pool, info.type));
        }

        if (pnfsIdInfo.getAttributes().getRetentionPolicy()
                        == RetentionPolicy.CUSTODIAL) {
            /*
             *  No need for panic, it has tertiary backup.
             */
            return;
        }

        singletons.add(pnfsIdInfo.pnfsId);
    }

    /*
     *  Launches a MakeCopies task and adds it to the futures queue.
     */
    @Override
    public synchronized void processCopy(PnfsIdInfo pnfsIdInfo)
                    throws InterruptedException {
        if (isDone()) {
            /*
             *  This will make sure the database access method
             *  which called back here is also interrupted.
             */
            throw new InterruptedException(String.format("%s, type %s, "
                            + "stopped.", info.pool, info.type));
        }

        LOGGER.debug("process copy, {}.", pnfsIdInfo);

        MakeCopies copyTask = new MakeCopies(info,
                                             poolGroupInfo.getPoolGroup().getName(),
                                             pnfsIdInfo,
                                             hub);
        taskFutures.add(copyTask.launch());
    }

    /*
     *  Launches a RemoveExtras task and adds it to the futures queue.
     */
    @Override
    public synchronized void processRemove(PnfsIdInfo pnfsIdInfo, int count)
                    throws InterruptedException, CacheException {
        if (isDone()) {
            /*
             *  This will make sure the database access method
             *  which called back here is also interrupted.
             */
            throw new InterruptedException(String.format("%s, type %s, "
                            + "stopped.", info.pool, info.type));
        }

        LOGGER.debug("process remove {}, {}.", count, pnfsIdInfo);

        RemoveExtras removeTask = new RemoveExtras(info, pnfsIdInfo, hub);
        taskFutures.add(removeTask.launch());
    }

    @Override
    protected void failed(Exception e) {
        LOGGER.error(GENERAL_FAILURE_MESSAGE, info.pool,
                        "process pool", ReplicationHub.exceptionMessage(e));
        failedAll();
    }

    /*
     * Single alarm for all files on the pool which have not been written
     * to backend storage (CUSTODIAL) but for which the pool contains
     * the unique copy.
     */
    private void sendAlarms() {
        if (!singletons.isEmpty()) {
            StringBuilder list = new StringBuilder();
            singletons.stream().forEach((id) -> list.append(id).append("\n"));
            LOGGER.error(AlarmMarkerFactory.getMarker(PredefinedAlarm.INACCESSIBLE_FILE,
                                                      info.pool),
                            INACCESSIBLE_FILE_MESSAGE,
                            info.pool,
                            list);
            singletons.clear();
        }
    }

    /*
     *  Initiates the scanning procedure by first using a heuristic
     *  to try to prune the potential result set of the query.  The
     *  query is then executed by a namespace access method which calls
     *  back to this task to process individual pnfsids requiring action.
     */
    private void processPnfsIds() {
        boolean exclude;
        String filter;
        try {
            switch (info.type) {
                case POOL_DOWN:
                    /*
                     *  Files with a location count less than the highest minimum
                     *  (excluding the down pool, hence the increment of 1)
                     *  for the pool group are then vetted on a storage-group basis.
                     *  The upper bound guarantees the less-than query will not
                     *  miss the special cases.
                     */
                    exclude = true;
                    filter = " < " + poolGroupInfo.getUpperBoundForMin() + 1;
                    break;
                case POOL_RESTART:
                    /*
                     *  Files with a location count greater than the lowest maximum
                     *  for the pool group are then vetted on a storage-group basis.
                     *  The lower bound guarantees the great-than query will not
                     *  miss the special cases.
                     */
                    exclude = false;
                    filter = " > " + poolGroupInfo.getLowerBoundForMax();
                    break;
                case POOL_SCAN:
                    /*
                     *  The cursor will run through all the pnfsids,
                     *  but only those in need of action will generate
                     *  a callback to this task.
                     */
                default:
                    exclude = false;
                    filter = null;
                    break;

            }

            PnfsInfoQuery query = new PnfsInfoQuery(info.pool,
                                                    poolGroupInfo,
                                                    this,
                                                    exclude);
            query.createSql(filter);

            /*
             *  This is sequential and will block until
             *  all the pnfsids returned by the query have
             *  been examined, and either discarded or
             *  sent back to one of the processing methods here.
             */
            access.handlePnfsidsForPool(query);
        } catch (CacheException | ParseException e) {
            failed(e);
        }
    }

    /*
     *  Creates a barrier on all launched subtasks.
     */
    private synchronized int waitForFutures() {
        int failed = 0;

        while(!isDone() && !taskFutures.isEmpty()) {
            Future future = taskFutures.remove();

            while (!future.isDone()) {
                try {
                    future.get(1, TimeUnit.MINUTES);
                } catch (InterruptedException | TimeoutException e) {
                    LOGGER.trace("Future get() for waitForFutures: {}.",
                                    ReplicationHub.exceptionMessage(e));
                } catch (ExecutionException e) {
                    LOGGER.trace("Future get() for waitForFutures: {}.",
                                    ReplicationHub.exceptionMessage(e));
                    ++failed;
                    break;
                }
            }
        }

        return failed + taskFutures.size();
    }
}
