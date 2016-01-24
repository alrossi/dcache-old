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

import org.junit.Before;
import org.junit.Test;

import diskCacheV111.pools.PoolV2Mode;
import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.PoolStatusChangedMessage;
import org.dcache.resilience.TestBase;
import org.dcache.resilience.TestSynchronousExecutor.Mode;
import org.dcache.resilience.data.PnfsFilter;
import org.dcache.resilience.data.PoolStateUpdate;
import org.dcache.resilience.data.PoolStatusForResilience;
import org.dcache.resilience.util.InaccessibleFileHandler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

/**
 * Created by arossi on 9/21/15.
 */
public class PoolOperationHandlerTest extends TestBase {
    String pool;
    int matchingFiles = 0;

    @Before
    public void setUp() throws CacheException, InterruptedException {
        setUpBase();
        setShortExecutionMode(Mode.NOP);
        setLongExecutionMode(Mode.NOP);
        createCounters();
        createPoolOperationHandler();
        createPoolOperationMap();
        setMocks();
        createPnfsOperationHandler();
        createPnfsOperationMap();
        initializeCounters();
        wirePoolOperationMap();
        wirePoolOperationHandler();
        wirePnfsOperationMap();
        wirePnfsOperationHandler();
        testNamespaceAccess.setHandler(pnfsOperationHandler);
        poolOperationMap.setRescanWindow(Integer.MAX_VALUE);
        poolOperationMap.setDownGracePeriod(0);
        poolOperationMap.loadPools();
    }

    @Test
    public void shouldNotSubmitUpdateWhenPoolIsNotResilient() {
        givenADownStatusChangeFor("standard_pool-0");
        assertNull(poolOperationMap.getState("standard_pool-0"));
    }

    @Test
    public void shouldNotSubmitUpdateWhenReadOnlyDownIsReceivedOnResilientPool() {
        givenAReadOnlyDownStatusChangeFor("resilient_pool-0");
        assertEquals("IDLE", poolOperationMap.getState(pool));
    }

    @Test
    public void shouldNotSubmitUpdateWhenUpIsReceivedOnResilientPool() {
        givenAnUpStatusChangeFor("resilient_pool-0");
        assertEquals("IDLE", poolOperationMap.getState(pool));
    }

    @Test
    public void shouldProcessOnlyFilesMatchingStorageUnitOnModifiedConstraints() {
        givenMinimumReplicasOnPool();
        givenAModificationOfConstraintsForAStorageUnit();
        whenPoolOpScanIsRun();

        /*
         * Only the files matching that storage unit.
         */
        theResultingNumberOfPnfsOperationsSubmittedWas(matchingFiles);
    }

    @Test
    public void shouldProcess0PnfsIdsWhenPoolWithMinReplicasRestarts() {
        givenMinimumReplicasOnPool();
        givenARestartStatusChangeFor(pool);
        whenPoolOpScanIsRun();
        theResultingNumberOfPnfsOperationsSubmittedWas(0);
    }

    @Test
    public void shouldProcess10FilesWhenPoolWithSingleReplicasGoesDown() {
        givenSingleReplicasOnPool();
        givenADownStatusChangeFor(pool);
        whenPoolOpScanIsRun();

        /*
         * 5 REPLICA ONLINE, 5 CUSTODIAL
         * (the inaccessible handler is invoked later, during
         * the verification phase)
         */
        theResultingNumberOfPnfsOperationsSubmittedWas(10);
    }

    @Test
    public void shouldProcess10PnfsIdsWhenPoolWithSingleReplicasRestarts() {
        givenSingleReplicasOnPool();
        givenARestartStatusChangeFor(pool);
        whenPoolOpScanIsRun();
        /*
         *  5 REPLICA ONLINE, 5 CUSTODIAL ONLINE
         */
        theResultingNumberOfPnfsOperationsSubmittedWas(10);
    }

    @Test
    public void shouldProcess4PnfsIdsWhenPoolWithExcessReplicasDown() {
        givenExcessReplicasOnPool();
        givenADownStatusChangeFor(pool);
        whenPoolOpScanIsRun();

        /*
         * 2 REPLICA ONLINE, 2 CUSTODIAL ONLINE
         */
        theResultingNumberOfPnfsOperationsSubmittedWas(4);
    }

    @Test
    public void shouldProcess4PnfsIdsWhenPoolWithMinReplicasDown() {
        givenMinimumReplicasOnPool();
        givenADownStatusChangeFor(pool);
        whenPoolOpScanIsRun();

        /*
         * 2 REPLICA ONLINE, 2 CUSTODIAL ONLINE
         */
        theResultingNumberOfPnfsOperationsSubmittedWas(4);
    }

    @Test
    public void shouldProcess6PnfsIdsWhenPoolWithExcessReplicasRestarts() {
        givenExcessReplicasOnPool();
        givenARestartStatusChangeFor(pool);
        whenPoolOpScanIsRun();

        /*
         * 3 REPLICA ONLINE, 3 CUSTODIAL ONLINE
         */
        theResultingNumberOfPnfsOperationsSubmittedWas(6);
    }

    @Test
    public void shouldSubmitUpdateWhenDownIsReceivedOnResilientPool() {
        givenADownStatusChangeFor("resilient_pool-0");
        assertEquals(PoolStatusForResilience.DOWN,
                        poolOperationMap.getLastStatus(pool));
        assertEquals("WAITING", poolOperationMap.getState(pool));
    }

    @Test
    public void shouldSubmitUpdateWhenRestartIsReceivedOnResilientPool() {
        givenARestartStatusChangeFor("resilient_pool-0");
        assertEquals(PoolStatusForResilience.RESTART,
                        poolOperationMap.getLastStatus(pool));
        assertEquals("WAITING", poolOperationMap.getState(pool));
    }

    @Test
    public void shouldSubmitUpdateWhenUpIsReceivedOnResilientPoolButWriteDisabled() {
        givenAnUpDisabledStrictStatusChangeFor("resilient_pool-0");
        assertEquals(PoolStatusForResilience.DOWN,
                        poolOperationMap.getLastStatus(pool));
        assertEquals("WAITING", poolOperationMap.getState(pool));
    }

    private void givenADownStatusChangeFor(String pool) {
        this.pool = pool;
        PoolStatusChangedMessage message = new PoolStatusChangedMessage(pool,
                        PoolStatusChangedMessage.DOWN);
        message.setPoolMode(new PoolV2Mode(PoolV2Mode.DISABLED_DEAD));
        PoolStateUpdate update = new PoolStateUpdate(message);
        poolInfoMap.updatePoolStatus(update);
        poolOperationHandler.handlePoolStatusChange(update);
    }

    private void givenAModificationOfConstraintsForAStorageUnit() {
        String unit = "resilient-1.dcache-devel-test@enstore";
        PoolStateUpdate update = new PoolStateUpdate(pool, unit);
        poolInfoMap.setGroupConstraints(unit, (short)3, "rack");
        matchingFiles = 2;
        poolOperationHandler.handlePoolStatusChange(update);
    }

    private void givenAReadOnlyDownStatusChangeFor(String pool) {
        this.pool = pool;
        PoolStatusChangedMessage message = new PoolStatusChangedMessage(pool,
                        PoolStatusChangedMessage.DOWN);
        message.setPoolMode(new PoolV2Mode(PoolV2Mode.DISABLED_RDONLY));
        PoolStateUpdate update = new PoolStateUpdate(message);
        poolInfoMap.updatePoolStatus(update);
        poolOperationHandler.handlePoolStatusChange(update);
    }

    private void givenARestartStatusChangeFor(String pool) {
        this.pool = pool;
        PoolStatusChangedMessage message = new PoolStatusChangedMessage(pool,
                        PoolStatusChangedMessage.RESTART);
        message.setPoolMode(new PoolV2Mode(PoolV2Mode.ENABLED));
        PoolStateUpdate update = new PoolStateUpdate(message);
        poolInfoMap.updatePoolStatus(update);
        poolOperationHandler.handlePoolStatusChange(update);
    }

    private void givenAnUpDisabledStrictStatusChangeFor(String pool) {
        this.pool = pool;
        PoolStatusChangedMessage message = new PoolStatusChangedMessage(pool,
                        PoolStatusChangedMessage.UP);
        message.setPoolMode(new PoolV2Mode(PoolV2Mode.DISABLED_STRICT));
        PoolStateUpdate update = new PoolStateUpdate(message);
        poolInfoMap.updatePoolStatus(update);
        poolOperationHandler.handlePoolStatusChange(update);
    }

    private void givenAnUpStatusChangeFor(String pool) {
        this.pool = pool;
        PoolStatusChangedMessage message = new PoolStatusChangedMessage(pool,
                        PoolStatusChangedMessage.UP);
        message.setPoolMode(new PoolV2Mode(PoolV2Mode.DISABLED_RDONLY));
        PoolStateUpdate update = new PoolStateUpdate(message);
        poolInfoMap.updatePoolStatus(update);
        poolOperationHandler.handlePoolStatusChange(update);
    }

    private void givenExcessReplicasOnPool() {
        loadFilesWithExcessLocations();
        pool = "resilient_pool-5";
    }

    private void givenMinimumReplicasOnPool() {
        loadFilesWithRequiredLocations();
        pool = "resilient_pool-13";
    }

    private void givenSingleReplicasOnPool() {
        loadNewFilesOnPoolsWithHostAndRackTags();
        pool = "resilient_pool-10";
    }

    private void setMocks() {
        setInaccessibleFileHandler(mock(InaccessibleFileHandler.class));
        setShortExecutionMode(Mode.NOP);
        setLongExecutionMode(Mode.RUN);
    }

    private void theResultingNumberOfPnfsOperationsSubmittedWas(int submitted) {
        PnfsFilter filter = new PnfsFilter();
        filter.setState("WAITING");
        assertEquals(submitted, pnfsOperationMap.count(filter));
    }

    private void whenPoolOpScanIsRun() {
        poolOperationMap.scan();
    }
}
