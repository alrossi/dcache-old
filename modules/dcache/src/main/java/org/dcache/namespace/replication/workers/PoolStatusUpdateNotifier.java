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
package org.dcache.namespace.replication.workers;

import diskCacheV111.vehicles.PoolStatusChangedMessage;
import org.dcache.namespace.replication.PoolStatusNotifier;
import org.dcache.namespace.replication.data.PoolStatusMessageType;

/**
 * Implementation of the object which is registered with the
 * {@link org.dcache.namespace.replication.caches.PoolStatusCache}.
 * It is owned by an instance of the PoolStatusUpdateWorker, and
 * reacts to any incoming state change messages affecting the pool
 * in question.  Requires a wait of a determinate interval before
 * actually allowing the worker to start.  This is to ensure that adequate
 * time has passed to confirm that the state of the pool (particularly
 * DOWN) is not just ephemeral.
 * <p/>
 * Handles a possible state switch from DOWN to RESTART or vice-versa
 * depending on the intervening messages received and the state of
 * the notifier itself.
 *
 * Created by arossi on 1/23/15.
 */
class PoolStatusUpdateNotifier implements Runnable, PoolStatusNotifier {
    /*
     * These are the "external" states associated with the type of
     * PoolStatusChangedMessage (DOWN, RESTART).  The worker implementation
     * can have its own set of (sub)states inside the RUNNING states.
     */
    enum State {
        DOWN_WAIT,          // worker waiting an interval before processing DOWN message
        RESTART_WAIT,       // worker waiting an interval before processing RESTART message
        DOWN_RUNNING,       // worker processing DOWN message
        RESTART_RUNNING,    // worker processing RESTART message
        DOWN_COMPLETED,     // worker finished processing DOWN message
        RESTART_COMPLETED   // worker finished processing RESTART message
    }

    private final long waitInterval;
    private final PoolStatusUpdateWorker owner;

    private String poolName;
    private PoolStatusMessageType lastReceived;
    private State current;
    private State next;

    PoolStatusUpdateNotifier(String poolName, PoolStatusUpdateWorker owner,
                    PoolStatusMessageType lastReceived, long waitInterval) {
        this.poolName = poolName;
        this.owner = owner;

        this.lastReceived = lastReceived;
        switch(lastReceived) {
            case DOWN:      current = State.DOWN_WAIT;      break;
            case RESTART:   current = State.RESTART_WAIT;   break;
            default:
        }

        this.waitInterval = waitInterval;
    }

    @Override
    public String getNotifierName() {
        return poolName + "-status-change-notifier";
    }

    @Override
    public String getPoolName() {
        return poolName;
    }

    @Override
    public void run() {
        long wait = waitInterval;
        while(true) {
            synchronized (this) {
                long beginWait = System.currentTimeMillis();
                LOGGER.debug("{}, before wait: current {}, next {}, wait {}.",
                                getNotifierName(), current, next, wait);
                try {
                    if (wait >= 0) {
                        wait(wait);
                    }
                } catch (InterruptedException e) {
                    LOGGER.debug("{}, wait was notified.", getNotifierName());
                    switch(current) {
                        case RESTART_WAIT:
                            if (lastReceived == PoolStatusMessageType.RESTART) {
                                /*
                                 * Pool RESTART has been succeeded by another
                                 * RESTART before waitInterval has passed;
                                 * just update the waitInterval.
                                 */
                                wait -= (System.currentTimeMillis()-beginWait);
                                continue;
                            }

                            /*
                             * Pool RESTART has been succeeded by a
                             * DOWN before waitInterval has passed;
                             * shift the task to DOWN and start over.
                             */
                            current = State.DOWN_WAIT;
                            wait = waitInterval;
                            continue;
                        case RESTART_RUNNING:
                            if (lastReceived == PoolStatusMessageType.RESTART) {
                                /*
                                 * Pool RESTART has been succeeded by another
                                 * RESTART while worker is handling RESTART;
                                 * just keep waiting until completion.
                                 */
                                wait = Long.MAX_VALUE; // wait for taskCompleted()
                                continue;
                            }

                            /*
                             * Pool RESTART has been succeeded by a
                             * DOWN while worker is handling RESTART;
                             * set the next state to DOWN_WAIT, and
                             * wait until current operation completes.
                             */
                            next = State.DOWN_WAIT;
                            wait = Long.MAX_VALUE; // wait for taskCompleted()
                            continue;
                        case DOWN_WAIT:
                            if (lastReceived == PoolStatusMessageType.DOWN) {
                                /*
                                 * Pool DOWN has been succeeded by another
                                 * DOWN before waitInterval has passed;
                                 * just update the waitInterval.
                                 */
                                wait -= (System.currentTimeMillis()-beginWait);
                                continue;
                            }

                            /*
                             * Pool DOWN has been succeeded by a
                             * RESTART before waitInterval has passed.
                             * Treat this as an ephemeral change and cancel
                             * the task altogether.
                             */
                            current = State.DOWN_COMPLETED;
                            owner.done();
                            return;
                        case DOWN_RUNNING:
                            if (lastReceived == PoolStatusMessageType.DOWN) {
                                /*
                                 * Pool DOWN has been succeeded by another
                                 * DOWN while worker is handling DOWN;
                                 * just keep waiting until completion.
                                 */
                                wait = Long.MAX_VALUE; // wait for taskCompleted()
                                continue;
                            }

                            /*
                             * Pool DOWN has been succeeded by a
                             * RESTART while worker is handling DOWN.
                             * set the next state to RESTART_WAIT, and
                             * wait until current operation completes.
                             */
                            next = State.RESTART_WAIT;
                            wait = Long.MAX_VALUE; // wait for taskCompleted()
                            continue;
                        case RESTART_COMPLETED:
                        case DOWN_COMPLETED:
                            if (next == null) {
                                /*
                                 * No further state changes to process.
                                 */
                                owner.unregisterNotifier();
                                return;
                            }

                            /*
                             * A message with the opposite state from the
                             * current one arrived while the worker was
                             * running.  The worker has completed, so
                             * we reset it to handle the new state
                             * after the normal interval has passed.
                             */
                            current = next;
                            next = null;
                            wait = waitInterval;
                            continue;
                    }
                }

                /*
                 * The wait has completed, so we start the worker.
                 * The worker will set its first state and queue itself
                 * appropriately, so it is not running on this thread.
                 */
                LOGGER.debug("{}, wait completed, starting worker: "
                                                + "current {}, next {}, type {}.",
                                getNotifierName(), current, next, lastReceived);
                owner.start(lastReceived);
            }
        }
    }

    @Override
    public synchronized void messageArrived(PoolStatusChangedMessage message) {
        PoolStatusMessageType type
                        = PoolStatusMessageType.valueOf(message.getPoolStatus());
        switch(type) {
            case RESTART:
            case DOWN:
                lastReceived = type;
                LOGGER.trace("{}, message arrived {}: notifying.",
                                getNotifierName(), message);
                notifyAll();
                break;
            case UNKNOWN:
            case UP:
            default:
                break;
        }
    }

    @Override
    public synchronized void taskCompleted() {
        switch(current) {
            case DOWN_RUNNING:
                current = State.DOWN_COMPLETED;
                LOGGER.debug("{}, worker finished: notifying {}.", current,
                                getNotifierName());
                notifyAll();
                break;
            case RESTART_RUNNING:
                current = State.RESTART_COMPLETED;
                LOGGER.debug("{}, worker finished: notifying {}.", current,
                                getNotifierName());
                notifyAll();
                break;
            case DOWN_COMPLETED:
            case RESTART_COMPLETED:
                break;
            case DOWN_WAIT:
            case RESTART_WAIT:
            default:
                throw new RuntimeException("taskCompleted() was called while"
                            + " in the " + current + " state; this is a bug.");
        }
    }
}
