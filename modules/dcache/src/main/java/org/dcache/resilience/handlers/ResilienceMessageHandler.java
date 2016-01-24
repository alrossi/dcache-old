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
package org.dcache.resilience.handlers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;

import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.AddStorageUnitToGroupsMessage;
import diskCacheV111.vehicles.AddStorageUnitsToGroupMessage;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.NotifyReplicaMessage;
import diskCacheV111.vehicles.PnfsClearCacheLocationMessage;
import diskCacheV111.vehicles.PoolStatusChangedMessage;
import diskCacheV111.vehicles.ReloadPsuMessage;
import diskCacheV111.vehicles.RemoveStorageUnitsFromGroupMessage;
import dmg.cells.nucleus.CellMessageReceiver;
import org.dcache.pool.migration.PoolMigrationCopyFinishedMessage;
import org.dcache.resilience.data.PnfsUpdate;
import org.dcache.resilience.data.MessageType;
import org.dcache.resilience.data.PoolInfoMap;
import org.dcache.resilience.data.PoolOperationMap;
import org.dcache.resilience.data.PoolStateUpdate;
import org.dcache.resilience.util.BackloggedMessageHandler;
import org.dcache.resilience.util.BrokenFileTask;
import org.dcache.resilience.util.MapInitializer;
import org.dcache.resilience.util.MessageGuard;
import org.dcache.resilience.util.MessageGuard.Status;
import org.dcache.resilience.util.OperationStatistics;
import org.dcache.util.ExceptionMessage;
import org.dcache.vehicles.CorruptFileMessage;
import org.dcache.vehicles.resilience.AddPoolToPoolGroupMessage;
import org.dcache.vehicles.resilience.ModifyStorageUnitMessage;
import org.dcache.vehicles.resilience.RemovePoolFromPoolGroupMessage;
import org.dcache.vehicles.resilience.RemovePoolGroupMessage;
import org.dcache.vehicles.resilience.RemovePoolMessage;
import org.dcache.vehicles.resilience.RemoveStorageUnitFromPoolGroupsMessage;
import org.dcache.vehicles.resilience.RemoveStorageUnitMessage;

/**
 * <p>All of the messages received here are subscribed to via topics.</p>
 *
 * Created by arossi on 8/3/15.
 */
public final class ResilienceMessageHandler
                implements CellMessageReceiver, BackloggedMessageHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(
                    ResilienceMessageHandler.class);

    private MessageGuard         messageGuard;
    private MapInitializer       initializer;
    private PnfsOperationHandler pnfsOperationHandler;
    private PoolOperationHandler poolOperationHandler;
    private PoolInfoMap          poolInfoMap;
    private PoolOperationMap     poolOpMap;
    private OperationStatistics  counters;
    private ExecutorService      updateService;

    public void messageArrived(AddPoolToPoolGroupMessage message) {
        /**
         * Scans the "new" pool, also making sure all files have the
         * sticky bit.
         */
        poolInfoMap.addToPoolGroup(message.pool, message.poolGroup);
        poolOpMap.remove(message.pool);
        poolOpMap.add(message.pool);
        Integer gindex = poolInfoMap.getGroupIndex(message.poolGroup);
        poolOperationHandler.getSubmitService()
                            .submit(() -> scanPool(message.pool, gindex, null));
    }

    public void messageArrived(AddStorageUnitToGroupsMessage message) {
        poolInfoMap.addStorageUnit(message.unit, message.groups);
    }

    public void messageArrived(AddStorageUnitsToGroupMessage message) {
        message.units.stream().forEach((u) -> {
            poolInfoMap.addStorageUnit(u, message.group);
        });
    }

    public void messageArrived(CorruptFileMessage message) {
        if (messageGuard.getStatus("CorruptFileMessage", message)
                        == Status.INACTIVE) {
            return;
        }
        counters.incrementMessage(MessageType.CORRUPT_FILE.name());
        new BrokenFileTask(message.pnfsId, message.pool,
                           pnfsOperationHandler).submit();
    }

    public void messageArrived(ModifyStorageUnitMessage message) {
        poolInfoMap.setGroupConstraints(message.unit, message.required,
                                        message.oneCopyPer);
        /*
         * A change to the unit requirements means that all pools which
         * have files belonging to this storage unit should be scanned.
         */
        for (Integer i : poolInfoMap.getPoolGroupsFor(message.unit)) {
            scanPoolsInGroup(poolInfoMap.getGroupName(i), message.unit);
        }
    }

    public void messageArrived(NotifyReplicaMessage message) {
        if (messageGuard.getStatus("NotifyReplicaMessage", message)
                        != Status.EXTERNAL) {
            return;
        }
        counters.incrementMessage(MessageType.NEW_FILE_LOCATION.name());
        updatePnfsLocation(new PnfsUpdate(message.pnfsId, message.pool,
                                          MessageType.NEW_FILE_LOCATION));
    }

    public void messageArrived(PnfsClearCacheLocationMessage message) {
        if (messageGuard.getStatus("PnfsClearCacheLocationMessage", message)
                        != Status.EXTERNAL) {
            return;
        }
        counters.incrementMessage(MessageType.CLEAR_CACHE_LOCATION.name());
        updatePnfsLocation(new PnfsUpdate(message.getPnfsId(),
                                          message.getPoolName(),
                                          MessageType.CLEAR_CACHE_LOCATION));
    }

    public void messageArrived(PoolStatusChangedMessage message) {
        messageGuard.incrementPoolStatusMessageCount();
        if (messageGuard.getStatus("PoolStatusChangedMessage", message)
                        == Status.INACTIVE) {
            return;
        }
        PoolStateUpdate update = new PoolStateUpdate(message);
        counters.incrementMessage(update.status.getMessageType().name());
        poolInfoMap.updatePoolStatus(update);
        poolOperationHandler.handlePoolStatusChange(update);
    }

    public void messageArrived(PoolMigrationCopyFinishedMessage message) {
        pnfsOperationHandler.handleMigrationCopyFinished(message);
    }

    public ReloadPsuMessage messageArrived(ReloadPsuMessage message) {
        LOGGER.trace("Received {}", message);
        initializer.reloadPsu(message.psu);
        message.setSucceeded();
        return message;
    }

    public void messageArrived(RemovePoolMessage message) {
        poolInfoMap.removePool(message.pool);
        poolOpMap.remove(message.pool);
    }

    public void messageArrived(RemovePoolGroupMessage message) {
        poolInfoMap.removeGroup(message.group);
    }

    public void messageArrived(RemovePoolFromPoolGroupMessage message) {
        /*
         *  NB:  if we try to scan the pool as DOWN, this means we need
         *  to pass the old group id for the pool, because we cannot
         *  synchronize the scan + copy tasks such as to create a barrier
         *  so that we can remove the pool from the map after
         *  everything completes.
         */
        Integer gindex = poolInfoMap.getGroupIndex(message.poolGroup);
        poolInfoMap.removeFromPoolGroup(message.pool, message.poolGroup);
        poolOperationHandler.getSubmitService()
                            .submit(() -> scanPool(message.pool, null, gindex));
    }

    public void messageArrived(RemoveStorageUnitMessage message) {
        poolInfoMap.removeGroup(message.unit);
    }

    public void messageArrived(RemoveStorageUnitFromPoolGroupsMessage message) {
        poolInfoMap.removeStorageUnit(message.unit, message.groups);
    }

    public void messageArrived(RemoveStorageUnitsFromGroupMessage message) {
        message.units.stream().forEach((u) -> {
            poolInfoMap.removeStorageUnit(u.getName(), message.group);
        });
    }

    @Override
    public void processBackloggedMessage(Message message) {
        if (message instanceof CorruptFileMessage) {
            messageArrived((CorruptFileMessage) message);
        } else if (message instanceof PnfsClearCacheLocationMessage) {
            messageArrived((PnfsClearCacheLocationMessage) message);
        } else if (message instanceof NotifyReplicaMessage) {
            messageArrived((NotifyReplicaMessage) message);
        } else if (message instanceof PoolStatusChangedMessage) {
            messageArrived((PoolStatusChangedMessage) message);
        } else if (message instanceof AddPoolToPoolGroupMessage) {
            messageArrived((AddPoolToPoolGroupMessage) message);
        } else if (message instanceof AddStorageUnitsToGroupMessage) {
            messageArrived((AddStorageUnitsToGroupMessage) message);
        } else if (message instanceof AddStorageUnitToGroupsMessage) {
            messageArrived((AddStorageUnitToGroupsMessage) message);
        } else if (message instanceof ModifyStorageUnitMessage) {
            messageArrived((ModifyStorageUnitMessage) message);
        } else if (message instanceof RemovePoolFromPoolGroupMessage) {
            messageArrived((RemovePoolFromPoolGroupMessage) message);
        } else if (message instanceof RemovePoolGroupMessage) {
            messageArrived((RemovePoolGroupMessage) message);
        } else if (message instanceof RemovePoolMessage) {
            messageArrived((RemovePoolMessage) message);
        } else if (message instanceof RemoveStorageUnitFromPoolGroupsMessage) {
            messageArrived((RemoveStorageUnitFromPoolGroupsMessage) message);
        } else if (message instanceof RemoveStorageUnitMessage) {
            messageArrived((RemoveStorageUnitMessage) message);
        } else if (message instanceof RemoveStorageUnitsFromGroupMessage) {
            messageArrived((RemoveStorageUnitsFromGroupMessage) message);
        }

        /*
         *  ReloadPsu messages received during the inactive phase are dropped.
         */
    }

    public void scanPool(String pool) {
        poolOperationHandler.getSubmitService()
                            .submit(() -> scanPool(pool, null, null));
    }

    public void setCounters(OperationStatistics counters) {
        this.counters = counters;
    }

    public void setInitializer(
                    MapInitializer initializer) {
        this.initializer = initializer;
    }

    public void setMessageGuard(MessageGuard messageGuard) {
        this.messageGuard = messageGuard;
    }

    public void setPnfsOperationHandler(
                    PnfsOperationHandler pnfsOperationHandler) {
        this.pnfsOperationHandler = pnfsOperationHandler;
    }

    public void setPoolInfoMap(PoolInfoMap poolInfoMap) {
        this.poolInfoMap = poolInfoMap;
    }

    public void setPoolOpMap(PoolOperationMap poolOpMap) {
        this.poolOpMap = poolOpMap;
    }

    public void setPoolOperationHandler(
                    PoolOperationHandler poolOperationHandler) {
        this.poolOperationHandler = poolOperationHandler;
    }

    public void setUpdateService(
                    ExecutorService updateService) {
        this.updateService = updateService;
    }

    private void scanPool(String pool, String unit) {
        PoolStateUpdate update = poolInfoMap.getPoolState(pool, unit);

        /*
         * Bypasses the transition check to place the operation
         * on the waiting queue.
         */
        poolOpMap.scan(update);
    }

    private void scanPool(String pool, Integer addedTo, Integer removedFrom) {
        PoolStateUpdate update
                        = poolInfoMap.getPoolState(pool, addedTo, removedFrom);
        poolInfoMap.updatePoolStatus(update);

        /*
         * Bypasses the transition check to place the operation
         * on the waiting queue.
         */
        poolOpMap.scan(update);
        LOGGER.trace("scanPool scan({}) called.", update);
    }

    private void scanPoolsInGroup(String poolGroupName, String unit) {
        poolInfoMap.getPoolsOfGroup(poolInfoMap.getGroupIndex(poolGroupName))
                   .stream()
                   .map((i) -> poolInfoMap.getPool(i))
                   .forEach((p) -> poolOperationHandler.getSubmitService()
                                                       .submit(() ->
                                                        scanPool(p, unit)));
    }

    private void updatePnfsLocation(PnfsUpdate data) {
        /*
         *  Database access occurs on this call, so it is better to
         *  dedicate a thread to this.
         */
        updateService.submit(() -> {
            try {
                pnfsOperationHandler.handleLocationUpdate(data);
            } catch (CacheException e) {
                LOGGER.error("Error in verification of location update data {}: {}",
                             data, new ExceptionMessage(e));
            }
        });
    }
}
