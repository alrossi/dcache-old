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
package org.dcache.replication.messages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PnfsModifyCacheLocationMessage;
import diskCacheV111.vehicles.PoolStatusChangedMessage;

import org.dcache.replication.api.PnfsCacheMessageType;
import org.dcache.replication.api.ReplicationEndpoints;
import org.dcache.replication.api.ReplicationMessageReceiver;
import org.dcache.replication.api.ReplicationOperationRegistry;
import org.dcache.replication.api.ReplicationQueryUtilities;
import org.dcache.replication.api.ReplicationRemoveMessageFactory;
import org.dcache.replication.api.ReplicationStatistics;
import org.dcache.replication.api.ReplicationTaskExecutor;
import org.dcache.replication.data.PnfsIdMetadata;
import org.dcache.replication.data.PoolMetadata;
import org.dcache.replication.runnable.PeriodicScanner;
import org.dcache.replication.tasks.ReducePnfsIdTask;
import org.dcache.vehicles.replication.CorruptFileMessage;
import org.dcache.vehicles.replication.ReplicationStatusMessage;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * The handler is responsible for processing all incoming messages of the
 * following types:
 * <br><br>
 * <table>
 *    <tr><td>{@link PnfsModifyCacheLocationMessage}</td></tr>
 *    <tr><td>{@link PoolStatusChangedMessage}</td></tr>
 *    <tr><td>{@link ReplicationStatusMessage}</td></tr>
 *    <tr><td>{@link CorruptFileMessage}</td></tr>
 * </table>
 *
 * @author arossi
 */
public final class SynchronizedMessageReceiver implements ReplicationMessageReceiver {
    private static final Logger LOGGER
        = LoggerFactory.getLogger(SynchronizedMessageReceiver.class);

    private ReplicationEndpoints hub;
    private ReplicationTaskExecutor executor;
    private ReplicationOperationRegistry map;
    private ReplicationRemoveMessageFactory factory;
    private ReplicationStatistics statistics;
    private ReplicationQueryUtilities utils;
    private PeriodicScanner scanner;

    private boolean startScanner = false;
    private long initialWait = 1;
    private TimeUnit initialWaitUnit = TimeUnit.MINUTES;

    private AtomicBoolean accept = new AtomicBoolean(false);

    /**
     * One-time alarm clock thread.  The message handler can
     * be paused for an initial period before beginning to process
     * intercepted messages.  This is useful because a cold start of
     * an entire system generates multiple pool state messages which
     * usually do not need to be processed.
     */
    private class AlarmClock extends Thread {
        @Override
        public void run() {
            long waitInMs = initialWaitUnit.toMillis(initialWait);
            try {
                Thread.sleep(waitInMs);
            } catch (InterruptedException ie ) {
                LOGGER.debug("Thread interrupted during initial wait.");
            }

            accept.set(true);

            if (startScanner) {
                scanner.initialize();
            }
        }
    }

    public void initialize() {
        checkNotNull(hub);
        checkNotNull(executor);
        checkNotNull(map);
        checkNotNull(factory);
        checkNotNull(statistics);
        checkNotNull(utils);
        checkNotNull(scanner);

        statistics.register(MESSAGES, ADD_CACHE_LOCATION_MSGS);
        statistics.register(MESSAGES, POOL_STATUS_CHANGED_MSGS);
        statistics.register(MESSAGES, COPY_JOB_STATUS_MSGS);
        statistics.register(MESSAGES, CLEAR_CACHE_LOCATION_MSGS);
        statistics.register(MESSAGES, CORRUPT_FILE_MSGS);

        new AlarmClock().start();
    }

    /**
     * Note that the handler treats "add" and "clear" indifferently.  That is,
     * on either message, a verification will be initiated which will reveal
     * the actual state of the replication count, and subsequently trigger
     * the appropriate action.  It is reasonable to expect that a situation
     * in which the user is actually trying to clear all cache locations
     * one-by-one, and which might risk triggering the restoration of other
     * (unwanted) copies, will be rare (and in any case we would advise against
     * taking such an action).
     */
    public synchronized void messageArrived(PnfsModifyCacheLocationMessage message) {
        if (!acceptMessage("Modify Cache Location", message)) {
           return;
        }

        PnfsCacheMessageType type = PnfsCacheMessageType.getType(message);

        if (type.equals(PnfsCacheMessageType.INDETERMINATE)) {
            LOGGER.debug("Message irrelevant to replication; skipping ...");
            return;
        }

        type.increment(statistics);

        PnfsIdMetadata opData;
        try {
            opData = new PnfsIdMetadata(message.getPnfsId(),
                                        message.getPoolName(),
                                        type,
                                        hub.getPoolMonitor()
                                           .getPoolSelectionUnit(),
                                        utils);
        } catch (CacheException | InterruptedException t) {
            LOGGER.error("{}: there was a problem checking"
                            + " for the pool group to which {} belongs: {}.",
                            message,
                            message.getPoolName(),
                            t.getMessage());

            type.incrementFailed(statistics);
            return;
        }

        if (opData.poolGroupData.poolGroup == null) {
            LOGGER.debug("{} does not belong to a replicating group, "
                            + "{} will not be replicated",
                            opData.poolName,
                            opData.pnfsId);
            return;
        }

        if (!map.isRegistered(opData)) {
            LOGGER.debug("{} register and verify.", opData);
            map.register(opData);
            executor.submitVerifyTask(opData);
        } else if (!type.equals(PnfsCacheMessageType.SCAN)) {
            LOGGER.debug("{} corresponds to an operation already in progress; "
                            + "updating operation data.",
                            opData);

            PnfsIdMetadata updated;

            try {
                updated = map.update(opData, message);
            } catch (CacheException | InterruptedException t) {
                LOGGER.error("{}: there was a problem updating data; attempting"
                                + " to cancel request.",
                                message,
                                message.getPoolName(),
                                t.getMessage());
                handleFailed(opData, message.getPoolName());
                type.incrementFailed(statistics);
                return;
            }

            if (updated != null && updated != opData) {
                /*
                 * A new object with the same key but alternate pool has been
                 * created (replacing the currently mapped one).  This
                 * only happens in the case of the removal of a corrupt file
                 * during a replication request.  A new replication task
                 * with the new source is submitted.
                 */
                executor.submitReplicateTask(updated);
            }
        }
    }

    public synchronized void messageArrived(PoolStatusChangedMessage message) {
        if (!acceptMessage("Pool Status Changed", message)) {
            return;
        }

        PoolMetadata poolData;
        try {
            poolData = new PoolMetadata(message.getPoolName(),
                                        hub.getPoolMonitor()
                                           .getPoolSelectionUnit(),
                                        utils);
        } catch (CacheException | InterruptedException t) {
            LOGGER.error("{}: there was a problem checking"
                            + " for the pool group to which {} belongs: {}.",
                            message,
                            message.getPoolName(),
                            t.getMessage());
            statistics.incrementFailed(MESSAGES, POOL_STATUS_CHANGED_MSGS);
            return;
        }

        try {
            switch(message.getPoolState()) {
                case PoolStatusChangedMessage.RESTART:
                    executor.submitScanForRedundantTask(poolData);
                    statistics.increment(MESSAGES, POOL_STATUS_CHANGED_MSGS);
                    break;
                case PoolStatusChangedMessage.DOWN:
                    executor.submitScanForDeficientTask(poolData);
                    statistics.increment(MESSAGES, POOL_STATUS_CHANGED_MSGS);
                    break;
                case PoolStatusChangedMessage.UP:
                default:
                    // NOP
            }
        } catch (InterruptedException | CacheException t) {
            LOGGER.error("Problem handling pool status change "
                            + "for {} {}: {}.",
                            message.getPoolName(),
                            message.getPoolStatus(),
                            t.getMessage());
            statistics.incrementFailed(MESSAGES, POOL_STATUS_CHANGED_MSGS);
        }
    }

    /**
     *  XXX (Emulated currently by the polling module, but this is only temporary.)
     */
    public synchronized void messageArrived(ReplicationStatusMessage message) {
        if (!acceptMessage("Replication Job Status", message)) {
            return;
        }

        /*
         * See the explanation in the javadoc for this
         * method.  The map should maintain consistency on itself
         * and remove terminated operations.
         */
        PnfsIdMetadata opData = map.update(message);

        if (opData != null) {
            switch (message.jobState) {
                case COPY_CANCELLED:
                    statistics.increment(MESSAGES, COPY_JOB_STATUS_MSGS);
                    handleCancelled(opData, message.pool);
                    break;
                case COPY_FAILED:
                    statistics.incrementFailed(MESSAGES, COPY_JOB_STATUS_MSGS);
                    handleFailed(opData, message.pool);
                    break;
                case CORRUPT_FILE:
                    statistics.increment(MESSAGES, COPY_JOB_STATUS_MSGS);
                    handleCorruptFile(message.pnfsid, message.pool);
                    break;
                default:
                    statistics.increment(MESSAGES, COPY_JOB_STATUS_MSGS);

            }
        }
    }

    public synchronized void messageArrived(CorruptFileMessage message) {
        if (!acceptMessage("Corrupt File Message", message)) {
            return;
        }

        statistics.increment(MESSAGES, CORRUPT_FILE_MSGS);
        handleCorruptFile(message.pnfsid, message.pool);
    }

    public void setExecutor(ReplicationTaskExecutor executor) {
        this.executor = executor;
    }

    public void setFactory(ReplicationRemoveMessageFactory factory) {
        this.factory = factory;
    }

    public void setHub(ReplicationEndpoints hub) {
        this.hub = hub;
    }

    public void setInitialWait(long initialWait) {
        this.initialWait = initialWait;
    }

    public void setInitialWaitUnit(TimeUnit initialWaitUnit) {
        this.initialWaitUnit = initialWaitUnit;
    }

    public void setMap(ReplicationOperationRegistry map) {
        this.map = map;
    }

    public void setScanner(PeriodicScanner scanner) {
        this.scanner = scanner;
    }

    public void setStartScanner(boolean startScanner) {
        this.startScanner = startScanner;
    }

    public void setStatistics(ReplicationStatistics statistics) {
        this.statistics = statistics;
    }

    public void setUtils(ReplicationQueryUtilities utils) {
        this.utils = utils;
    }

    private boolean acceptMessage(String message, Object messageObject) {
        LOGGER.trace("************* Replication {}: {}.", message, messageObject);
        if (!accept.get()) {
            LOGGER.trace("Replica Manager message handler is paused, "
                            + "message will be dropped.");
            return false;
        }
        return true;
    }

    /*
     * TODO currently behavior is undetermined.  Note that this method
     *      will not implement any state changes on the operations map;
     *      the latter are handled internally by the map itself.
     */
    private void handleCancelled(PnfsIdMetadata opData, String pool) {
        LOGGER.debug("{}/{}, handleCancelled not yet implemented.", opData,
                                                                    pool);
    }

    /*
     * TODO currently behavior is undetermined. Note that this method
     *      will not implement any state changes on the operations map;
     *      the latter are handled internally by the map itself.
     */
    private void handleFailed(PnfsIdMetadata opData, String pool) {
        LOGGER.debug("{}, handleFailed not yet implemented.", opData,
                                                              pool);
    }

    /*
     * Calls the same code as the {@link ReducePnfsIdTask}.
     */
    private void handleCorruptFile(PnfsId pnfsid, String pool) {
        LOGGER.debug("{} {}, handleCorruptFile.", pnfsid, pool);
        ReducePnfsIdTask.removeCopy(pnfsid,
                                    pool,
                                    this,
                                    factory,
                                    hub,
                                    statistics);
    }
}
