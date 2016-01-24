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

import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import diskCacheV111.poolManager.StorageUnit;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.AddStorageUnitToGroupsMessage;
import diskCacheV111.vehicles.AddStorageUnitsToGroupMessage;
import diskCacheV111.vehicles.NotifyReplicaMessage;
import diskCacheV111.vehicles.PnfsClearCacheLocationMessage;
import diskCacheV111.vehicles.PoolStatusChangedMessage;
import diskCacheV111.vehicles.RemoveStorageUnitsFromGroupMessage;
import org.dcache.pool.migration.PoolMigrationCopyFinishedMessage;
import org.dcache.resilience.TestBase;
import org.dcache.resilience.TestSynchronousExecutor;
import org.dcache.resilience.TestSynchronousExecutor.Mode;
import org.dcache.resilience.data.MessageType;
import org.dcache.resilience.data.PoolInfoMap;
import org.dcache.resilience.data.PoolOperationMap;
import org.dcache.resilience.data.PoolStateUpdate;
import org.dcache.resilience.util.MessageGuard;
import org.dcache.resilience.util.MessageGuard.Status;
import org.dcache.resilience.util.OperationStatistics;
import org.dcache.resilience.util.OperationStatistics.CounterType;
import org.dcache.vehicles.resilience.AddPoolToPoolGroupMessage;
import org.dcache.vehicles.resilience.ModifyStorageUnitMessage;
import org.dcache.vehicles.resilience.RemovePoolFromPoolGroupMessage;
import org.dcache.vehicles.resilience.RemovePoolGroupMessage;
import org.dcache.vehicles.resilience.RemovePoolMessage;
import org.dcache.vehicles.resilience.RemoveStorageUnitFromPoolGroupsMessage;
import org.dcache.vehicles.resilience.RemoveStorageUnitMessage;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by arossi on 9/21/15.
 */
public final class ResilienceMessageHandlerTest {

    ResilienceMessageHandler handler;
    MessageGuard             messageGuard;
    PnfsOperationHandler     pnfsOperationHandler;
    PoolOperationHandler     poolOperationHandler;
    PoolInfoMap              poolInfoMap;
    PoolOperationMap         poolOpMap;
    OperationStatistics      counters;
    PoolStateUpdate          update;

    @Before
    public void setUp() {
        messageGuard = mock(MessageGuard.class);
        pnfsOperationHandler = mock(PnfsOperationHandler.class);
        poolOperationHandler = mock(PoolOperationHandler.class);
        poolInfoMap = mock(PoolInfoMap.class);
        poolOpMap = mock(PoolOperationMap.class);
        counters = new OperationStatistics();
        counters.setStatisticsPath(TestBase.STATSFILE);
        counters.initialize();
        handler = new ResilienceMessageHandler();
        handler.setCounters(counters);
        handler.setMessageGuard(messageGuard);
        handler.setPnfsOperationHandler(pnfsOperationHandler);
        handler.setPoolInfoMap(poolInfoMap);
        handler.setPoolOperationHandler(poolOperationHandler);
        handler.setPoolOpMap(poolOpMap);
        handler.setUpdateService(new TestSynchronousExecutor(Mode.RUN));
        when(poolOperationHandler.getSubmitService())
                        .thenReturn(new TestSynchronousExecutor(Mode.RUN));
    }

    @After
    public void shutDown() {
        new File(TestBase.STATSFILE).delete();
    }

    @Test
    public void verifyAddPoolToPoolGroupMessage() {
        AddPoolToPoolGroupMessage message = new AddPoolToPoolGroupMessage(
                        "test-pool", "test-group");
        handler.processBackloggedMessage(message);
        verifyScanPool("test-pool", true, false, false);
    }

    @Test
    public void verifyAddStorageUnitToGroupsMessage() {
        Collection<String> groups = new ArrayList<>();
        groups.add("test-group");
        StorageUnit unit = new StorageUnit("test-unit");
        AddStorageUnitToGroupsMessage message = new AddStorageUnitToGroupsMessage(
                        unit, groups);
        handler.processBackloggedMessage(message);
        verify(poolInfoMap).addStorageUnit(unit, groups);
    }

    @Test
    public void verifyAddStorageUnitsToGroupMessage() {
        StorageUnit unit = new StorageUnit("test-unit");
        Collection<StorageUnit> units = new ArrayList<>();
        units.add(unit);
        AddStorageUnitsToGroupMessage message = new AddStorageUnitsToGroupMessage(
                        "test-group", units);
        handler.processBackloggedMessage(message);
        verify(poolInfoMap).addStorageUnit(unit, "test-group");
    }

    @Test
    public void verifyModifyStorageUnitMessage() {
        short required = 2;
        ModifyStorageUnitMessage message
                        = new ModifyStorageUnitMessage("test-unit", required,
                                                       null);
        update = new PoolStateUpdate("test-pool", "test-unit");
        when(poolInfoMap.getStorageUnitsFor("test-group")).thenReturn(
                        ImmutableList.of(2));
        when(poolInfoMap.getGroupName(2)).thenReturn("test-unit");
        when(poolInfoMap.getGroupName(0)).thenReturn("test-group");
        when(poolInfoMap.getGroupIndex("test-group")).thenReturn(0);
        when(poolInfoMap.getGroupIndex("test-unit")).thenReturn(2);
        when(poolInfoMap.getPoolsOfGroup(0)).thenReturn(ImmutableList.of(1));
        when(poolInfoMap.getPoolGroupsFor("test-unit")).thenReturn(
                        ImmutableList.of(0));
        when(poolInfoMap.getPool(1)).thenReturn("test-pool");
        when(poolInfoMap.getPoolState("test-pool", "test-unit")).thenReturn(
                        update);
        handler.processBackloggedMessage(message);
        verify(poolInfoMap).setGroupConstraints("test-unit", required, null);
        verifyScanPool("test-pool", false, false, true);
    }

    @Test
    public void verifyNotifyReplicaMessage() throws CacheException {
        PnfsId pnfsId = new PnfsId("0000000000000000BCD");
        NotifyReplicaMessage message = new NotifyReplicaMessage(pnfsId,
                                                                "test-pool");
        when(messageGuard.getStatus("NotifyReplicaMessage",
                                    message)).thenReturn(Status.EXTERNAL);
        handler.processBackloggedMessage(message);
        /*
         *  Without artificially altering the call structure of the
         *  handler, verification of the actual pnfsOperationHandler method
         * call is not possible here as the message handler creates an
         * anonymous struct-type object on the fly to pass as parameter.
         */
        assertEquals(1, counters.getCount(CounterType.MESSAGE.name(),
                                          MessageType.NEW_FILE_LOCATION.name(),
                                          false));
    }

    @Test
    public void verifyPnfsClearCacheLocationMessage() {
        PnfsId pnfsId = new PnfsId("0000000000000000BCD");
        PnfsClearCacheLocationMessage message = new PnfsClearCacheLocationMessage(
                        pnfsId, "test-pool");
        when(messageGuard.getStatus("PnfsClearCacheLocationMessage",
                                    message)).thenReturn(Status.EXTERNAL);
        handler.processBackloggedMessage(message);
        /*
         *  Without artificially altering the call structure of the
         *  handler, verification of the actual pnfsOperationHandler method
         * call is not possible here as the message handler creates an
         * anonymous struct-type object on the fly to pass as parameter.
         */
        assertEquals(1, counters.getCount(CounterType.MESSAGE.name(),
                        MessageType.CLEAR_CACHE_LOCATION.name(), false));
    }

    @Test
    public void verifyPoolMigrationCopyFinishedMessage() {
        PoolMigrationCopyFinishedMessage message = new PoolMigrationCopyFinishedMessage(
                        UUID.randomUUID(), "test-pool", new PnfsId("0000000000000000BCD"));
        handler.messageArrived(message);
        verify(pnfsOperationHandler).handleMigrationCopyFinished(message);
    }

    @Test
    public void verifyPoolStatusChangedMessage() {
        PoolStatusChangedMessage message = new PoolStatusChangedMessage(
                        "test-pool", PoolStatusChangedMessage.DOWN);
        when(messageGuard.getStatus("PoolStatusChangedMessage",
                        message)).thenReturn(Status.EXTERNAL);
        handler.processBackloggedMessage(message);
        /*
         *  Without artificially altering the call structure of the
         *  handler, verification of the actual scanPool method
         *  call is not possible here as the message handler creates an
         *  anonymous struct-type object on the fly to pass as parameter.
         */
        assertEquals(1, counters.getCount(CounterType.MESSAGE.name(),
                        MessageType.POOL_STATUS_DOWN.name(), false));
    }

    @Test
    public void verifyRemovePoolFromPoolGroupMessage() {
        RemovePoolFromPoolGroupMessage message
                        = new RemovePoolFromPoolGroupMessage("test-pool",
                                                             "test-group");
        handler.processBackloggedMessage(message);
        verify(poolInfoMap).getGroupIndex("test-group");
        verify(poolInfoMap).removeFromPoolGroup("test-pool", "test-group");
        verifyScanPool("test-pool", false, true, false);
    }

    @Test
    public void verifyRemovePoolGroupMessage() {
        RemovePoolGroupMessage message = new RemovePoolGroupMessage("test-group");
        handler.processBackloggedMessage(message);
        verify(poolInfoMap).removeGroup("test-group");
    }

    @Test
    public void verifyRemovePoolMessage() {
        RemovePoolMessage message = new RemovePoolMessage("test-pool");
        handler.processBackloggedMessage(message);
        verify(poolInfoMap).removePool("test-pool");
        verify(poolOpMap).remove("test-pool");
    }

    @Test
    public void verifyRemoveStorageUnitFromPoolGroupsMessage() {
        List<String> groups = ImmutableList.of("test-group");
        RemoveStorageUnitFromPoolGroupsMessage message =
                        new RemoveStorageUnitFromPoolGroupsMessage("test-unit", groups);
        handler.processBackloggedMessage(message);
        verify(poolInfoMap).removeStorageUnit("test-unit", groups);
    }

    @Test
    public void verifyRemoveStorageUnitMessage() {
        RemoveStorageUnitMessage message = new RemoveStorageUnitMessage(
                        "test-unit");
        handler.processBackloggedMessage(message);
        verify(poolInfoMap).removeGroup(message.unit);
    }

    @Test
    public void verifyRemoveStorageUnitsFromGroupMessage() {
        List<StorageUnit> units = ImmutableList.of(new StorageUnit("test-unit"));
        RemoveStorageUnitsFromGroupMessage message =
                        new RemoveStorageUnitsFromGroupMessage("test-group", units);
        handler.processBackloggedMessage(message);
        verify(poolInfoMap).removeStorageUnit("test-unit", "test-group");
    }

    private void verifyScanPool(String pool, boolean add, boolean remove, boolean modify) {
        if (modify) {
            verify(poolInfoMap).getPoolState(pool, update.storageUnit);
        } else if (add) {
            verify(poolInfoMap).getPoolState(pool, 0, null);
            verify(poolInfoMap).updatePoolStatus(update);
        } else if (remove) {
            verify(poolInfoMap).getPoolState(pool, null, 0);
            verify(poolInfoMap).updatePoolStatus(update);
        } else {
            verify(poolInfoMap).getPoolState(pool, null, null);
            verify(poolInfoMap).updatePoolStatus(update);
        }

        verify(poolOpMap).scan(update);
    }
}
