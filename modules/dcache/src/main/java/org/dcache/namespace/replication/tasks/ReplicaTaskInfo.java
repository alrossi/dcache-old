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

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import diskCacheV111.util.PnfsId;
import org.dcache.namespace.replication.data.PoolStatusMessageType;

/**
 * This class serves two purposes.  The first is to encapsulate
 * basic task data, such as pool name, pnfsid, and type of
 * overall operation.  The second is to serve as a kind of
 * monitor for cancelling and/or waiting for completion of
 * the entire operation.
 * </p>
 * Note that there are two
 * {@link org.dcache.namespace.replication.tasks.ReplicaTaskFuture}
 * objects associated with this class; one is the future exported
 * by this object itself; the other is the future of the currently
 * running "master task".  The latter is there so that cancellation
 * of the full execution tree is possible via cancel on this object.
 * By "master task" is meant a task which waits on any subtasks
 * it spawns (these include
 * {@link org.dcache.namespace.replication.tasks.VerifyPool},
 * {@link org.dcache.namespace.replication.tasks.ProcessPnfsId}
 * and
 * {@link org.dcache.namespace.replication.tasks.ProcessPool}).
 * </p>
 * For tracking purposes, each instance of this class has a
 * unique id and a name associated with it.  The latter is
 * either the name of the pool for pool tasks, or the pnfsid
 * for the single replication process.  These are used to register
 * and unregister the task info.  The registry can then be
 * used to inspect and cancel running operations.
 * </p>
 *
 * Created by arossi on 2/7/15.
 */
public final class ReplicaTaskInfo implements Cancellable {

    public enum Type {
        SINGLE, POOL_DOWN, POOL_RESTART, POOL_SCAN
    }

    /**
     *  Should uniquely identify this task, so as to allow
     *  for registration and eventual cancellation.
     */
    public final UUID taskId = UUID.randomUUID();

    /*
     *  In case a caller needs to wait for the entire
     *  task to finish.
     */
    public final ReplicaTaskFuture<ReplicaTaskInfo> replicaTaskInfoFuture;

    public final String pool;

    /*
     *  Will be <code>null</code> when the task is bound to
     *  the processing of a pool.
     */
    public final PnfsId pnfsId;

    private final AtomicBoolean done = new AtomicBoolean(false);
    private final AtomicBoolean cancelled = new AtomicBoolean(false);

    /*
     * It is possible for the pool status notifier to reset this.
     * We allow only tasks direct access to this field.
     */
    Type type;

    /*
     * This will be the CompletionFuture of one of the following:
     *
     * (a) VerifyPool
     * (b) ProcessPnfsId
     * (c) ProcessPool
     *
     * In case (b), the pnfsid process task waits for the copy and
     * remove tasks.
     *
     * In case (c), the pool process task is associated with a notifier,
     * and maintains a barrier on all the spawned copy and remove tasks.
     * Stopping the pool processing via cancel will cancel those futures.
     */
    private ReplicaTaskFuture<? extends ReplicaTask> currentTaskFuture;

    /*
     * When the task is bound to the processing of a pool,
     * a sentinel is used to intercept intervening messages
     * regarding that pool.
     */
    private PoolMessageSentinel sentinel;

    public ReplicaTaskInfo(String pool) {
        this(null, pool);
        type = Type.POOL_SCAN;
    }

    public ReplicaTaskInfo(String pool, PoolStatusMessageType type) {
        this(null, pool);
        switch(type) {
            case DOWN:
                this.type = Type.POOL_DOWN;
                break;
            case RESTART:
                this.type = Type.POOL_RESTART;
                break;
            default:
                throw new IllegalArgumentException(String.format("No TaskInfo "
                                + "possible for pool status message type %s.",
                                type));
        }
    }

    public ReplicaTaskInfo(PnfsId pnfsId, String pool) {
        this(pool, pnfsId);
        type = Type.SINGLE;
    }

    private ReplicaTaskInfo(String pool, PnfsId pnfsId) {
        this.pool = pool;
        this.pnfsId = pnfsId;
        replicaTaskInfoFuture = new ReplicaTaskFuture<>(this);
    }

    @Override
    public boolean cancel() {
        if (isDone()) {
            return true;
        }

        if (cancelled.getAndSet(true)) {
            return true;
        };

        synchronized (this) {
            if (currentTaskFuture != null) {
                currentTaskFuture.cancel(true);
            }
        }

        done();

        return cancelled.get();
    }

    public void done() {
        if (done.getAndSet(true)) {
            return;
        }

        synchronized (this) {
            if (sentinel != null) {
                sentinel.done();
            }

            notifyAll();
        }
    }

    public Type getType() {
        return type;
    }

    public String getName() {
        if (pnfsId != null) {
            return pnfsId.toString();
        }
        return pool;
    }

    @Override
    public boolean isCancelled() {
        return cancelled.get();
    }

    @Override
    public boolean isDone() {
        return done.get() || cancelled.get();
    }

    public synchronized void
        setTaskFuture(ReplicaTaskFuture<? extends ReplicaTask> taskFuture) {
        currentTaskFuture = taskFuture;
    }

    public synchronized void setSentinel(PoolMessageSentinel sentinel) {
        this.sentinel = sentinel;
    }

    @Override
    public String toString() {
        return "TaskInfo("
                        + type + ", "
                        + pool
                        + (pnfsId == null ? "" : ", " +  pnfsId)
                        + ")";
    }
}
