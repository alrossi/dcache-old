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

import diskCacheV111.vehicles.PoolStatusChangedMessage;
import org.dcache.namespace.replication.ReplicaManagerHub;
import org.dcache.namespace.replication.data.PoolStatusMessageType;

/**
 * Implementation of the object which is registered with the
 * {@link org.dcache.namespace.replication.caches.PoolStatusCache}.
 * It reacts to any incoming state change messages affecting the pool
 * in question.  Requires a wait of a determinate interval before
 * actually allowing the task to start.  This is to ensure that adequate
 * time has passed to confirm that the state of the pool (particularly
 * DOWN) is not just ephemeral.
 * <p/>
 * Handles a possible state switch from DOWN to RESTART or vice-versa
 * depending on the intervening messages received and the state of
 * the sentinel itself.
 * </p>
 * The sentinel will launch the next phase of the operation
 * when the appropriate state is reached, and will then wait on its
 * future for completion.  If further action is required,
 * it will create a new task info object and relaunch.
 *
 * Created by arossi on 1/23/15.
 */
final class PoolUpdateSentinel implements Runnable, PoolMessageSentinel {
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


    private final ReplicaManagerHub hub;
    private final long waitInterval;

    private ReplicaTaskInfo info;
    private PoolStatusMessageType lastReceived;
    private State current;
    private State next;

    PoolUpdateSentinel(ReplicaTaskInfo info, ReplicaManagerHub hub) {
        this.info = info;
        this.hub = hub;
        this.waitInterval = hub.getPoolStatusGracePeriodUnit()
                               .toMillis(hub.getPoolStatusGracePeriod());

        switch(lastReceived) {
            case DOWN:      current = State.DOWN_WAIT;      break;
            case RESTART:   current = State.RESTART_WAIT;   break;
            default:
        }
    }

    @Override
    public String getName() {
        return info.pool + "-status-change-notifier";
    }

    @Override
    public String getPoolName() {
        return info.pool;
    }

    @Override
    public void run() {
        long wait = waitInterval;
        while(true) {
            synchronized (this) {
                long beginWait = System.currentTimeMillis();
                LOGGER.debug("{}, before wait: current {}, next {}, wait {}.",
                                getName(), current, next, wait);
                try {
                    if (wait >= 0) {
                        wait(wait);
                    }
                } catch (InterruptedException e) {
                    LOGGER.debug("{}, wait was notified.", getName());
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
                            info.type = ReplicaTaskInfo.Type.POOL_DOWN;
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
                                wait = Long.MAX_VALUE; // wait for done()
                                continue;
                            }

                            /*
                             * Pool RESTART has been succeeded by a
                             * DOWN while worker is handling RESTART;
                             * set the next state to DOWN_WAIT, and
                             * wait until current operation completes.
                             */
                            next = State.DOWN_WAIT;
                            wait = Long.MAX_VALUE; // wait for done()
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
                            info.cancel(); // should call back
                            wait = Long.MAX_VALUE; // wait for done()
                            continue;
                        case DOWN_RUNNING:
                            if (lastReceived == PoolStatusMessageType.DOWN) {
                                /*
                                 * Pool DOWN has been succeeded by another
                                 * DOWN while worker is handling DOWN;
                                 * just keep waiting until completion.
                                 */
                                wait = Long.MAX_VALUE; // wait for done()
                                continue;
                            }

                            /*
                             * Pool DOWN has been succeeded by a
                             * RESTART while worker is handling DOWN.
                             * set the next state to RESTART_WAIT, and
                             * wait until current operation completes.
                             */
                            next = State.RESTART_WAIT;
                            wait = Long.MAX_VALUE; // wait for done()
                            continue;
                        case RESTART_COMPLETED:
                        case DOWN_COMPLETED:
                            if (next == null) {
                                /*
                                 * No further state changes to process.
                                 */
                                hub.getPoolStatusCache()
                                   .unregisterPoolSentinel(this);
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
                            PoolStatusMessageType messageType;
                            switch(current) {
                                case DOWN_WAIT:
                                    messageType = PoolStatusMessageType.DOWN;
                                    break;
                                case RESTART_WAIT:
                                    messageType = PoolStatusMessageType.RESTART;
                                    break;
                                default:
                                    throw new IllegalArgumentException(current
                                                    + " cannot be the next state "
                                                    + "after completion of "
                                                    + "a task; this is a bug.");
                            }
                            info = new ReplicaTaskInfo(info.pool, messageType);
                            info.setSentinel(this);
                            wait = waitInterval;
                            continue;
                    }
                }

                /*
                 * The wait has completed, so we start the next phase.
                 * The phases are chained, so that the last will call done()
                 * on the notifier attached to the task info.
                 */
                LOGGER.debug("{}, wait completed, starting worker: "
                                                + "current {}, next {}, type {}.",
                                getName(), current, next, lastReceived);
                info.setTaskFuture(new ProcessPool(info, hub).launch());
            }
        }
    }

    @Override
    public synchronized void done() {
        switch(current) {
            case DOWN_WAIT:
            case DOWN_RUNNING:
                current = State.DOWN_COMPLETED;
                LOGGER.debug("{}, task finished: notifying {}.", current,
                                getName());
                notifyAll();
                break;
            case RESTART_RUNNING:
                current = State.RESTART_COMPLETED;
                LOGGER.debug("{}, task finished: notifying {}.", current,
                                getName());
                notifyAll();
                break;
            case DOWN_COMPLETED:
            case RESTART_COMPLETED:
                break;
            case RESTART_WAIT:
            default:
                throw new RuntimeException("taskCompleted() was called while"
                                + " in the " + current + " state; this is a bug.");
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
                                getName(), message);
                notifyAll();
                break;
            case UNKNOWN:
            case UP:
            default:
                break;
        }
    }

    @Override
    public synchronized void start() {
        info.setSentinel(this);
        hub.getPoolStatusCache().registerPoolSentinel(this);
        info.setTaskFuture(new ProcessPool(info, hub).launch());
        LOGGER.debug("{} started.", getName());
    }
}
