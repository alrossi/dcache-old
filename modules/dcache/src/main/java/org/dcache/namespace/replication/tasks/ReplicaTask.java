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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.dcache.namespace.replication.ReplicaManagerHub;
import org.dcache.util.replication.CollectionElementSelectionStrategy;

/**
 * Encapsulates fields and functions common to both pnfsid and pool status
 * tasks.
 * <p/>
 * This includes the base implementation of the Cancellable API, in order
 * to serve as delegate to
 * {@link org.dcache.namespace.replication.tasks.ReplicaTaskFuture}.
 *
 * Created by arossi on 1/27/15.
 */
public abstract class ReplicaTask implements Cancellable, Runnable {
    protected static final Logger LOGGER
                    = LoggerFactory.getLogger(ReplicaTask.class);

    protected static final String WILL_RETRY_LATER
                    = "Replication cannot proceed at this time; a best effort "
                    + "at retry will be made during the next periodic watchdog "
                    + "scan.";

    protected static final String GENERAL_FAILURE_MESSAGE
                    = "Processing for source {} failed during {}; {}."
                    + WILL_RETRY_LATER;

    /*
     *  Chooses randomly from the elements in the collection.  Note
     *  that this does not side-effect the collection itself.
     */
    protected static final CollectionElementSelectionStrategy<String> randomSelector
                    = new CollectionElementSelectionStrategy<String>() {

        private Random random = new Random(System.currentTimeMillis());

        public String select(Collection<String> collection) {
            if (collection.isEmpty()) {
                return null;
            }

            int index = Math.abs(random.nextInt()) % collection.size();

            Iterator<String> it = collection.iterator();

            for (int i = 0; i < index - 1; i++) {
                it.next();
            }

            return it.next();
        }
    };

    /*
     *  Needed to access caches, executor services and the namespace.
     */
    protected final ReplicaManagerHub hub;

    /*
     *  A wrapper for basic task information, providing a cancellable
     *  object with a future bound to the entire task execution.
     */
    protected final ReplicaTaskInfo info;

    private final AtomicBoolean cancelled = new AtomicBoolean(false);
    private final AtomicBoolean done = new AtomicBoolean(false);

    /**
     * The future associated with this task returned by the
     * submit() call to its executor service.
     */
    protected Future runFuture;

    protected ReplicaTask(ReplicaTaskInfo info, ReplicaManagerHub hub) {
        this.info = info;
        this.hub = hub;
        hub.getRegistry().register(info);
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName()
                        + "<--"
                        + info;
    }

    /*
     * ************************ Cancellable API *************************
     */

    /*
     * This cancel call should only be made through the
     * ReplicaTaskFuture.  Since that future is constructed
     * and returned by the launch method for the task, after
     * the task has been submitted to the queue, the internal
     * future returned by the executor should not be null here.
     */
    @Override
    public boolean cancel() {
        if (isDone()) {
            return true;
        }

        if (cancelled.getAndSet(true)) {
            return true;
        }

        runFuture.cancel(true);

        completed();

        LOGGER.debug("{} cancelled.", this);

        return cancelled.get();
    }

    @Override
    public boolean isCancelled() {
        return cancelled.get();
    }

    @Override
    public boolean isDone() { return done.get() || cancelled.get(); }

    /**
     * The contract on this method foresees that the task will
     * submit itself to its appropriate queue, and then construct
     * and return a future through which it can be joined on or cancelled.
     *
     * @return future connected to the execution of this task.
     */
    public abstract ReplicaTaskFuture<? extends ReplicaTask> launch();

    /*
     * This particular task has finished, but may belong to
     * a chain which is still active.  There may be waiter
     * on its own future, so notify must be called.
     */
    protected void completed() {
        done.set(true);
        synchronized (this) {
            notifyAll();
        }
        LOGGER.debug("{} completed called.", this);
    }

    /*
     * Called when the entire sequence of tasks constituting
     * the initiated procedure is deemed successfully terminated.
     * Signals any caller waiting on the process (via the info object)
     * as a whole, by setting the info object to done.
     */
    protected void completedAll() {
        completed();
        info.done();
        hub.getRegistry().unregister(info);
        LOGGER.debug("{} completedAll called.", this);
    }

    /*
     * Called when the entire sequence of tasks constituting
     * the initiated procedure is deemed unsuccessfully terminated.
     * Signals any caller waiting on the process (via the info object)
     * as a whole, by setting the info object to cancel.
     */
    protected void failedAll() {
        completed();
        info.cancel();
        hub.getRegistry().failed(info);
        LOGGER.debug("{} failedAll called.", this);
    }

    protected abstract void failed(Exception e);

    protected static String exceptionMessage(Exception e) {
        Throwable t = e.getCause();
        return String.format("Exception: %s%s.", e.getMessage(),
                        t == null ? "" : String.format(", cause: %s", t));
    }
}
