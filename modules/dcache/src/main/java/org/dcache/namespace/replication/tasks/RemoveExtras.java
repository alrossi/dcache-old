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

import com.google.common.base.Preconditions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.dcache.alarms.AlarmMarkerFactory;
import org.dcache.alarms.PredefinedAlarm;
import org.dcache.namespace.replication.ReplicaManagerHub;
import org.dcache.namespace.replication.data.PnfsIdInfo;
import org.dcache.namespace.replication.data.PoolGroupInfo;
import org.dcache.namespace.replication.db.LocalNamespaceAccess;
import org.dcache.util.replication.CellStubFactory;
import org.dcache.vehicles.replication.RemoveReplicaMessage;
import org.dcache.vehicles.replication.StickyReplicaMessage;

/**
 * Task which controls the removal of excess replicas.
 * </p>
 * Redundant copies are removed via a call to the replica handler
 * on the pool in question.  The cache entry state is actually set to
 * removed in the repository (as if rep rm -f were called), because of the
 * policy enforced concerning the presence of a sticky record owned by system
 * for all files on resilient pools (if removal were left to the sweeper, the
 * pool would be in an inconsistent state for some interval).
 * </p>
 * All replicas of the file are first pinned by the replica manager using
 * a proprietary sticky record so that any externally initiated removal during
 * this phase will not succeed; those sticky records are removed when the
 * operation completes.
 * </p>
 * Failure to remove a replica will not trigger an alarm, but failure to
 * unpin a copy will.
 *
 * Created by arossi on 2/7/15.
 */
public final class RemoveExtras extends ReplicaTask {
    protected static final String FAILED_REMOVE_MESSAGE
                    = "{}. \n"
                    + "This means that unnecessary copies may still exist; "
                    + "A best effort at removal will be made during "
                    + "the next periodic watchdog scan.";

    private static final String FAILED_UNPIN_MESSAGE
                    = "{}. \n"
                    + "The sticky records belonging to the replica manager "
                    + "may be removed manually using the admin command; "
                    + "if left, they will not expire for (at most) 12 hours.";

    private final PnfsIdInfo pnfsIdInfo;
    private final Collection<String> confirmed;

    private CellStubFactory stubFactory;
    private LocalNamespaceAccess access;

    public RemoveExtras(ReplicaTaskInfo info,
                        PnfsIdInfo pnfsIdInfo,
                        ReplicaManagerHub hub) {
        this(info, pnfsIdInfo, Collections.EMPTY_SET, hub);
    }

    public RemoveExtras(ReplicaTaskInfo info,
                        PnfsIdInfo pnfsIdInfo,
                        Collection<String> confirmed,
                        ReplicaManagerHub hub) {
        super(info, hub);
        this.pnfsIdInfo = pnfsIdInfo;
        this.confirmed = Preconditions.checkNotNull(confirmed);
    }

    /*
     *  There are less unproblematic opportunities to slice
     *  the execution of this task with checks to see if it has been
     *  cancelled or otherwise terminated, as we have done elsewhere.
     *
     *  But given that the number of locations handled will be fairly
     *  limited, since this task is bound to a single pnfsId, the
     *  task should complete in a reasonable amount of time.
     */
    @Override
    public void run() {
        selectAndRemove();
    }

    @Override
    public ReplicaTaskFuture<RemoveExtras> launch() {
        runFuture = hub.getRemoveExtrasExecutor().submit(this);
        return new ReplicaTaskFuture<>(this);
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName()
                        + "["
                        + pnfsIdInfo.pnfsId
                        + "]<--"
                        + info;
    }

    @Override
    protected void failed(Exception e) {
        LOGGER.error(FAILED_REMOVE_MESSAGE, e.getMessage());
        failedAll();
    }

    /**
     * Adds a sticky record with replica manager as owner on the pnfsid
     * in each location. This is done via a message
     * sent to the replica manager handler on the pool itself.
     *
     * The future returned from the send call is stored and
     * get() is then called on each, creating a barrier.
     *
     * Any pins which fails will cause an exception to be thrown
     * for immediate rollback of all pins.
     */
    private void pin(Collection<String> locations) throws Exception {
        Collection<Future<StickyReplicaMessage>> toJoin = new ArrayList<>();
        StickyReplicaMessage msg;

        for (String location : locations) {
            msg = new StickyReplicaMessage(location, info.pnfsId, true);
            LOGGER.trace("Sending StickyReplicasMessage {}.", msg);
            toJoin.add(stubFactory.getPoolStub(location).send(msg));
        }

        for (Future<StickyReplicaMessage> future : toJoin) {
            try {
                msg = future.get();
            } catch (InterruptedException | ExecutionException e) {
               /*
                *  This is here because of the Future API, but in the
                *  case of these calls, the exceptions have been
                *  suppressed.  The exception is wrapped into the
                *  message. If an exception is thrown
                *  here, there is a bug in the code.
                */
                throw new RuntimeException("An unexpected exception was "
                                + "thrown during a pin operation.", e);
            }

            LOGGER.trace("Returned StickyReplicasMessage {}.", msg);

            Serializable e = msg.getErrorObject();

            if (e != null) {
                /*
                 *  Fail-fast.
                 *  Everything should be rolled back by the unpin call
                 *  in the finally() of the calling method.
                 */
                throw new Exception(String.format("Failed pin of %s on %s: %s.",
                                    msg.pnfsId,
                                    msg.pool,
                                    exceptionMessage((Exception)e)));
            }
        }
    }

    /*
     * Removes from the given locations the cache entries of the pnfsid
     * associated with this task.  This is done via a message sent to a
     * handler for this purpose on the pool itself.
     *
     * The future returned from each send call is stored and
     * get() is then called on each, creating a barrier.
     *
     * When the call succefully completes, the location is removed
     * from the collection.  This way, we will know which locations
     * might need to be unpinned because of failure.
     */
    private void remove(Collection<String> locations) {
        Collection<Future<RemoveReplicaMessage>> toJoin = new ArrayList<>();
        RemoveReplicaMessage msg;

        for (String location : locations) {
            msg = new RemoveReplicaMessage(location, info.pnfsId);
            LOGGER.trace("Sending RemoveReplicasMessage {}.", msg);
            toJoin.add(stubFactory.getPoolStub(location).send(msg));
        }

        for (Future<RemoveReplicaMessage> future : toJoin) {
            try {
                msg = future.get();
            } catch (InterruptedException | ExecutionException e) {
               /*
                *  This is here because of the Future API, but in the
                *  case of these calls, the exceptions have been
                *  suppressed.  The exception is wrapped into the
                *  message. If an exception is thrown
                *  here, there is a bug in the code.
                */
                throw new RuntimeException("An unexpected exception was "
                                + "thrown during a remove operation.", e);
            }

            LOGGER.trace("Returned RemoveReplicasMessage {}.", msg);

            Serializable e = msg.getErrorObject();

            if (e == null) {
                locations.remove(msg.pool);
            } else {
                /*
                 *  Leave in the locations list.  A single message
                 *  for all will be sent.
                 */
            }
        }

        if (!locations.isEmpty()) {
            StringBuilder details = new StringBuilder(String.format("Was unable "
                                            + "to remove the following locations "
                                            + "for %s.\n",
                            info.pnfsId));
            for (String location: locations) {
                details.append("\t").append(location).append("\n");
            }

           /*
            * An alarm is not necessary here, but the location
            * is not removed from the list, so an attempt to unpin
            * may be made.
            */
            LOGGER.error(FAILED_REMOVE_MESSAGE, details);
        }
    }

    /*
     * Keep removing locations from the info location list
     * for the given pnfsId, adding entries to the list to remove
     * until we reach the limit.  It is necessary to remove
     * the selected location from the location list as well,
     * else the selector might select it more than once.
    */
    private Collection<String> select(Collection<String> locations, int limit) {
        Collection<String> toRemove = new ArrayList<>();
        for (int i = 0; i < limit; i++ ) {
            /*
             *  We might want to change this to proportional TODO
             */
            String location = randomSelector.select(locations);
            locations.remove(location);
            toRemove.add(location);
        }
        LOGGER.debug("Select completed for {} by selecting for removal {}.",
                        info.pnfsId, toRemove);
        return toRemove;
    }

    /*
     * All locations are pinned in advance of doing the remove computations.
     *
     * The logic for this is as follows:  By refreshing locations here, any
     * location removed after that call but before we pin the files will
     * result in a failed pin. So as not to complicate things, any
     * failed pin will immediately roll back the entire operation.
     *
     * It is expected that by isolating pin/unpin to single replica sets,
     * failures will be easier to control and there will be less risk
     * of leaving files pinned by the replica system.
     *
     * Note that any location newly created while this task is running need not
     * be handled here, as it would spawn a new update message which
     * should be intercepted and translated into a new task.
     */
    private void selectAndRemove() {
        Collection<String> toPin = new ArrayList<>();
        Collection<String> toRemove = null;

        try {
            /*
             * Refresh the file locations.
             */
            Collection<String> locations = pnfsIdInfo.refreshLocations(access);

            /*
             * Pin all
             */
            toPin.addAll(locations);
            pin(toPin);

            /*
             * Get list of all active pools and the current list of pools
             * in the group.
             */
            Collection<String> active = hub.getPoolInfoCache().findAllActivePools();

            PoolGroupInfo poolGroupInfo =
                            hub.getPoolInfoCache().getPoolGroupInfo(info.pool);
            Set<String> pgPools = poolGroupInfo.getPoolNames();

            /*
             * Remove inactive or no longer valid pools.
             */
            for (Iterator<String> it = locations.iterator(); it.hasNext();) {
                String location = it.next();
                if (!active.contains(location) || !pgPools.contains(location)) {
                    it.remove();
                }
            }

            int valid = locations.size();
            int max = pnfsIdInfo.getMaximum();

            if (valid > max) {
                /*
                 * Select locations after eliminating any confirmed locations.
                 */
                locations.removeAll(confirmed);
                toRemove = select(locations, max - confirmed.size());

                /*
                 * Remove the candidates from the pin list.
                 */
                toPin.removeAll(toRemove);

                /*
                 * Will remove the successful ones from the list.
                 */
                remove(toRemove);
            }

            completed();
        } catch (Exception t) {
            failed(t);
        } finally {
            if (toRemove != null) {
                /*
                 * Catch failed removes.
                 */
                toPin.addAll(toRemove);
            }

            /*
             * Unpin the remaining locations.
             * If unpins fail here, an alarm will be sent,
             * but the task will not be considered failed.
             */
            unpin(toPin);
        }
    }

    /**
     * Removes a sticky record with replica manager as owner on the pnfsids
     * on each location. This is done via a message
     * sent to the replica manager handler on the pool itself.
     *
     * The future returned from the send call is stored and
     * get() is then called on each, creating a barrier.
     *
     * Failed unpins provoke an alarm. Note that, in any case,
     * the Replica Manager sticky record has an expiration of 12 hours.
     */
    private void unpin(Collection<String> locations) {
        Collection<Future<StickyReplicaMessage>> toJoin = new ArrayList<>();
        StickyReplicaMessage msg;

        for (String location : locations) {
            msg = new StickyReplicaMessage(location, info.pnfsId, false);
            LOGGER.trace("Sending StickyReplicasMessage {}.", msg);
            toJoin.add(stubFactory.getPoolStub(location).send(msg));
        }

        for (Future<StickyReplicaMessage> future : toJoin) {
            try {
                msg = future.get();
            } catch (InterruptedException | ExecutionException e) {
               /*
                *  This is here because of the Future API, but in the
                *  case of these calls, the exceptions have been
                *  suppressed.  The exception is wrapped into the
                *  message. If an exception is thrown
                *  here, there is a bug in the code.
                */
                throw new RuntimeException("An unexpected exception was "
                                + "thrown during a pin operation.", e);
            }

            LOGGER.trace("Returned StickyReplicasMessage {}.", msg);

            Serializable e = msg.getErrorObject();

            if (e == null) {
                locations.remove(msg.pool);
            }

            /*
             *  else, leave in the locations list.  A single alarm
             *  for all will be sent.
             */
        }

        if (!locations.isEmpty()) {
            StringBuilder details = new StringBuilder(String.format("Was unable "
                                            + "to unpin the following locations for %s.\n",
                            info.pnfsId));
            for (String location: locations) {
                details.append("\t").append(location).append("\n");
            }

           /*
            *  This should generate an alarm, because we don't want
            *  to leave sticky records blocking all removal.
            */
            LOGGER.error(AlarmMarkerFactory.getMarker(PredefinedAlarm.FAILED_REPLICATION,
                                            info.pnfsId.toString()),
                            FAILED_UNPIN_MESSAGE,
                            details);
        }
    }
}
