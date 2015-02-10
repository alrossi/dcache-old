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
package org.dcache.namespace.replication;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PnfsClearCacheLocationMessage;
import diskCacheV111.vehicles.PoolStatusChangedMessage;
import dmg.cells.nucleus.CellMessageReceiver;
import org.dcache.namespace.replication.caches.PoolStatusCache;
import org.dcache.namespace.replication.data.PoolStatusMessageType;
import org.dcache.namespace.replication.monitoring.ActivityRegistry;
import org.dcache.namespace.replication.tasks.ReplicaTaskInfo;
import org.dcache.namespace.replication.tasks.VerifyPool;
import org.dcache.vehicles.PnfsSetFileAttributes;

/**
 * Receiver which is responsible for initiating the process of maintaining
 * the proper number of replicas for either a single pnfsid in a resilient
 * pool group, or for a pool in the group which has changed status.
 * <p/>
 * The same procedure is used when either an attribute update message or
 * a clear cache location message is intercepted.
 * <p/>
 * If the message passes the guard check, the replica handler's
 * session id will have been set on the diagnostic context (CDC).
 *
 * Created by arossi on 1/25/15.
 */
public final class ReplicationMessageHandler implements CellMessageReceiver {
    private static final Logger LOGGER
                    = LoggerFactory.getLogger(ReplicationMessageHandler.class);

    private static final String MESSAGES = "MESSAGES";
    private static final String CLEAR_CACHE_LOCATION = "CLEAR_CACHE_LOCATION";
    private static final String NEW_FILE_LOCATION = "NEW_FILE_LOCATION";
    private static final String POOL_STATUS_CHANGED = "POOL_STATUS_CHANGED";

    private MessageGuard guard;
    private ReplicationHub hub;
    private ActivityRegistry registry;

    public void initialize() {
        Preconditions.checkNotNull(guard);
        Preconditions.checkNotNull(hub);
        registry = hub.getRegistry();
    }

    public void messageArrived(PnfsClearCacheLocationMessage message) {
        /*
         * Guard check is done on the message queue thread
         * (there should be little overhead).
         */
        if (!guard.acceptMessage("Clear Cache Location", message)) {
            return;
        }

        registry.register(message);

        PnfsId pnfsId = message.getPnfsId();
        String pool = message.getPoolName();

        /*
         * When launch() is called the task immediately
         * queues itself onto its executor queue.
         */
        new VerifyPool(new ReplicaTaskInfo(pnfsId, pool), hub).launch();
        LOGGER.debug("Launched task for {}, {}.", pool, pnfsId);
    }

    public void messageArrived(PnfsSetFileAttributes message) {
        /*
         * Guard check is done on the message queue thread
         * (there should be little overhead).
         */
        if (!guard.acceptMessage("Set File Attributes", message)) {
            return;
        }

        PnfsId pnfsId = message.getPnfsId();
        Collection<String> locations = message.getFileAttributes().getLocations();

        /*
         * We are only interested in attribute updates where a single new
         * location is added.
         */
        if (locations.size() != 1) {
            LOGGER.debug("Message for {} contains {} locations ({}): "
                                            + "irrelevant to replication; "
                                            + "ignoring.",
                            pnfsId, locations.size(), locations);
            return;
        }

        registry.register(message);

        String pool = locations.iterator().next();

        /*
         * When launch() is called the task immediately
         * queues itself onto its executor queue.
         */
        new VerifyPool(new ReplicaTaskInfo(pnfsId, pool), hub).launch();
        LOGGER.debug("Launched task for {}, {}.", pool, pnfsId);
    }

    public void messageArrived(PoolStatusChangedMessage message) {
        PoolStatusMessageType type
                        = PoolStatusMessageType.valueOf(message.getPoolStatus());

        PoolStatusCache cache = hub.getPoolStatusCache();

        if (!type.isValidForUpdate()) {
            LOGGER.trace("{} is of type {}; ignoring.", message, type);
            return;
        }

        /*
         * Guard check is done on the message queue thread
         * (there should be little overhead).
         */
        if (!guard.acceptMessage("PoolStatusChangeHandler", message)) {
            return;
        }

        registry.register(message);

        String pool = message.getPoolName();

        /*
         * If this message regards a pool currently being processed for
         * status change, do nothing here.
         */
        if (hub.getPoolStatusCache().sentinelReceivedMessage(message)) {
            LOGGER.trace("{} is already being handled for status change; "
                            + "ignoring {}.",
                            pool, message);
            return;
        }

        /*
         * When launch() is called the task immediately
         * queues itself onto its executor queue.
         */
        new VerifyPool(new ReplicaTaskInfo(pool, type), hub).launch();
        LOGGER.debug("Launched status task for {}, {}.", pool, type);
    }

    public void setGuard(MessageGuard guard) {
        this.guard = guard;
    }

    public void setHub(ReplicationHub hub) {
        this.hub = hub;
    }
}
