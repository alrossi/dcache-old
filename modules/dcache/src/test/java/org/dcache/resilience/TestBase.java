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
package org.dcache.resilience;

import org.junit.After;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import diskCacheV111.pools.PoolV2Mode;
import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.Message;
import org.dcache.cells.CellStub;
import org.dcache.pool.classic.Cancellable;
import org.dcache.pool.migration.ProportionalPoolSelectionStrategy;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.resilience.data.PnfsOperationMap;
import org.dcache.resilience.data.PoolInfoMap;
import org.dcache.resilience.data.PoolOperationMap;
import org.dcache.resilience.data.PoolStateUpdate;
import org.dcache.resilience.data.PoolStatusForResilience;
import org.dcache.resilience.handlers.PnfsOperationHandler;
import org.dcache.resilience.handlers.PnfsTaskCompletionHandler;
import org.dcache.resilience.handlers.PoolOperationHandler;
import org.dcache.resilience.handlers.PoolTaskCompletionHandler;
import org.dcache.resilience.util.InaccessibleFileHandler;
import org.dcache.resilience.util.OperationStatistics;
import org.dcache.resilience.util.OperationHistory;
import org.dcache.resilience.util.PoolSelectionUnitDecorator;
import org.dcache.util.CellStubFactory;
import org.dcache.vehicles.FileAttributes;

import static org.mockito.Mockito.mock;

/**
 * Created by arossi on 9/21/15.
 */
public abstract class TestBase implements Cancellable {
    public static final String    STATSFILE      = "/tmp/statistics-file";

    protected static final Logger    LOGGER         = LoggerFactory.getLogger(
                    TestBase.class);
    protected static final Exception FORCED_FAILURE = new Exception(
                    "Forced failure for test purposes");
    protected static final String    CHKPTFILE      = "/tmp/checkpoint-file";

    /*
     *  Real instances.
     */
    protected CellStubFactory     cellStubFactory;
    protected OperationStatistics counters;

    /*
     *  Used, but also tested separately.
     */
    protected PnfsOperationHandler pnfsOperationHandler;
    protected PoolOperationHandler poolOperationHandler;
    protected PnfsOperationMap     pnfsOperationMap;
    protected PoolOperationMap     poolOperationMap;
    protected PoolInfoMap          poolInfoMap;

    /*
     *  Injected or created by individual tests.
     */
    protected InaccessibleFileHandler    inaccessibleFileHandler;
    protected PoolSelectionUnitDecorator decorator;

    protected PnfsTaskCompletionHandler pnfsTaskCompletionHandler;
    protected PoolTaskCompletionHandler poolTaskCompletionHandler;

    protected TestSynchronousExecutor shortJobExecutor;
    protected TestSynchronousExecutor longJobExecutor;
    protected TestSynchronousExecutor scheduledExecutorService;

    protected TestSelectionUnit testSelectionUnit;
    protected TestStub          testPnfsManagerStub;

    protected TestCostModule      testCostModule;
    protected TestPoolMonitor     testPoolMonitor;
    protected TestNamespaceAccess testNamespaceAccess;

    private boolean isCancelled = false;
    private boolean isDone      = false;

    /*
     * Whether the tasks should fail or cancel.
     */
    private TestSynchronousExecutor.Mode shortTaskExecutionMode = TestSynchronousExecutor.Mode.NOP;
    private TestSynchronousExecutor.Mode longTaskExecutionMode  = TestSynchronousExecutor.Mode.NOP;
    private TestSynchronousExecutor.Mode scheduledExecutionMode = TestSynchronousExecutor.Mode.NOP;

    private TestStub testPoolStub;

    @Override
    public void cancel() {
        isCancelled = true;
    }

    @After
    public void shutDown() {
        clearAll();
    }

    private void clearAll() {
        clearInMemory();
        if (testNamespaceAccess != null) {
            testNamespaceAccess.clear();
            testNamespaceAccess = null;
        }
        File file = new File(CHKPTFILE);
        if (file.exists()) {
            file.delete();
        }
        file = new File(STATSFILE);
        if (file.exists()) {
            file.delete();
        }
    }

    private void setLongTaskExecutor() {
        longJobExecutor = new TestSynchronousExecutor(longTaskExecutionMode);
    }

    private void setScheduledExecutor() {
        scheduledExecutorService = new TestSynchronousExecutor(
                        scheduledExecutionMode);
    }

    private void setShortTaskExecutor() {
        shortJobExecutor = new TestSynchronousExecutor(shortTaskExecutionMode);
    }

    protected FileAttributes aCustodialNearlineFile() throws CacheException {
        return testNamespaceAccess.getRequiredAttributes(
                        TestData.CUSTODIAL_NEARLINE[0]);
    }

    protected FileAttributes aCustodialOnlineFile() throws CacheException {
        return testNamespaceAccess.getRequiredAttributes(
                        TestData.CUSTODIAL_ONLINE[0]);
    }

    protected FileAttributes aDeletedReplicaOnlineFileWithBothTags()
                    throws CacheException {
        FileAttributes attributes = testNamespaceAccess.getRequiredAttributes(
                        TestData.REPLICA_ONLINE[3]);
        testNamespaceAccess.delete(attributes.getPnfsId(), false);
        attributes.getLocations().clear();
        return attributes;
    }

    protected FileAttributes aFileWithAReplicaOnAllResilientPools()
                    throws CacheException {
        FileAttributes attributes = aReplicaOnlineFileWithNoTags();
        attributes.setLocations(testCostModule.pools.stream().filter(
                        (p) -> p.contains("resilient")).collect(
                        Collectors.toList()));
        return attributes;
    }

    protected FileAttributes aNonResilientFile() throws CacheException {
        return testNamespaceAccess.getRequiredAttributes(
                        TestData.CUSTODIAL_NEARLINE[0]);
    }

    protected FileAttributes aReplicaOnlineFileWithBothTags()
                    throws CacheException {
        return testNamespaceAccess.getRequiredAttributes(
                        TestData.REPLICA_ONLINE[3]);
    }

    protected FileAttributes aReplicaOnlineFileWithBothTagsButNoLocations()
                    throws CacheException {
        FileAttributes attributes = testNamespaceAccess.getRequiredAttributes(
                        TestData.REPLICA_ONLINE[3]);
        testNamespaceAccess.delete(attributes.getPnfsId(), true);
        return testNamespaceAccess.getRequiredAttributes(
                        TestData.REPLICA_ONLINE[3]);
    }

    protected FileAttributes aReplicaOnlineFileWithHostTag()
                    throws CacheException {
        return testNamespaceAccess.getRequiredAttributes(
                        TestData.REPLICA_ONLINE[0]);
    }

    protected FileAttributes aReplicaOnlineFileWithNoTags()
                    throws CacheException {
        return testNamespaceAccess.getRequiredAttributes(
                        TestData.REPLICA_ONLINE[4]);
    }

    protected FileAttributes aReplicaOnlineFileWithRackTag()
                    throws CacheException {
        return testNamespaceAccess.getRequiredAttributes(
                        TestData.REPLICA_ONLINE[2]);
    }

    protected void clearInMemory() {
        counters = null;
        if (pnfsOperationMap != null) {
            pnfsOperationMap.shutdown();
            pnfsOperationMap = null;
        }
        if (poolOperationMap != null) {
            poolOperationMap.shutdown();
            poolOperationMap = null;
        }
        poolInfoMap = null;
        if (shortJobExecutor != null) {
            shortJobExecutor.shutdown();
            shortJobExecutor = null;
        }
        if (longJobExecutor != null) {
            longJobExecutor.shutdown();
            longJobExecutor = null;
        }
        testSelectionUnit = null;
        testCostModule = null;
        testPoolMonitor = null;
    }

    protected void createAccess() {
        /**
         * Some tests may try to simulate restart against a persistent namespace
         */
        if (testNamespaceAccess == null) {
            testNamespaceAccess = new TestNamespaceAccess();
        }
    }

    protected void createCellStubFactory() {
        cellStubFactory = new CellStubFactory() {
            @Override
            public CellStub getPoolStub(String destination) {
                return testPoolStub;
            }
        };
    }

    protected void createCellStubs() {
        testPnfsManagerStub = new TestStub();
        testPoolStub = new TestStub();
    }

    protected void createCostModule() {
        testCostModule = new TestCostModule();
    }

    protected void createCounters() {
        counters = new OperationStatistics();
    }

    protected void createPnfsOperationHandler() {
        pnfsOperationHandler = new PnfsOperationHandler();
        pnfsTaskCompletionHandler = new PnfsTaskCompletionHandler();
    }

    protected void createPnfsOperationMap() {
        pnfsOperationMap = new PnfsOperationMap();
    }

    protected void createPoolInfoMap() {
        poolInfoMap = new PoolInfoMap() {
            @Override
            protected PoolMonitor getPoolMonitor() {
                return testPoolMonitor;
            }
        };
    }

    protected void createPoolMonitor() {
        testPoolMonitor = new TestPoolMonitor();
    }

    protected void createPoolOperationHandler() {
        poolOperationHandler = new PoolOperationHandler();
        poolTaskCompletionHandler = new PoolTaskCompletionHandler();
    }

    protected void createPoolOperationMap() {
        poolOperationMap = new PoolOperationMap();
    }

    protected void createPsuDecorator() {
        decorator = new PoolSelectionUnitDecorator();
    }

    protected void createSelectionUnit() {
        testSelectionUnit = new TestSelectionUnit();
    }

    protected void initializeCostModule() {
        testCostModule.load();
    }

    protected void initializeCounters() {
        counters.setStatisticsPath(STATSFILE);
        counters.initialize();
        for (String pool: testSelectionUnit.getActivePools()) {
            counters.registerPool(pool);
        }
        counters.registerPool("UNDEFINED");
    }

    protected void initializePoolInfoMap() {
        poolInfoMap.reload(testSelectionUnit);
        testSelectionUnit.getAllDefinedPools(false).stream().forEach((p) -> {
            PoolStateUpdate update = new PoolStateUpdate(p.getName(),
                                                         PoolStatusForResilience.UP,
                                                         new PoolV2Mode(PoolV2Mode.ENABLED));
            poolInfoMap.updatePoolStatus(update);
        });
    }

    protected void initializePsuDecorator() {
        decorator.afterSetup();
    }

    protected void initializeSelectionUnit() {
        testSelectionUnit.load();
    }

    protected void loadFilesWithExcessLocations() {
        testNamespaceAccess.loadExcessResilient();
    }

    protected void loadFilesWithRequiredLocations() {
        testNamespaceAccess.loadRequiredResilient();
    }

    protected void loadNewFilesOnPoolsWithHostAndRackTags() {
        testNamespaceAccess.loadNewResilientOnHostAndRackTagsDefined();
    }

    protected void loadNewFilesOnPoolsWithHostTags() {
        testNamespaceAccess.loadNewResilientOnHostTagDefined();
    }

    protected void loadNewFilesOnPoolsWithNoTags() {
        testNamespaceAccess.loadNewResilient();
    }

    protected void loadNonResilientFiles() {
        testNamespaceAccess.loadNonResilient();
    }

    protected void makeNonResilient(String unit) {
        testSelectionUnit.makeStorageUnitNonResilient(unit);
        poolInfoMap.setGroupConstraints(unit, (short) 1, null);
    }

    protected void offlinePools(String... pool) {
        testSelectionUnit.setOffine(pool);
        for (String p : pool) {
            PoolStateUpdate update = new PoolStateUpdate(p,
                                                         PoolStatusForResilience.DOWN,
                                                         new PoolV2Mode(PoolV2Mode.DISABLED_STRICT));
            poolInfoMap.updatePoolStatus(update);
        }
    }

    protected void setInaccessibleFileHandler(InaccessibleFileHandler handler) {
        inaccessibleFileHandler = handler;
    }

    protected void setLongExecutionMode(TestSynchronousExecutor.Mode mode) {
        longTaskExecutionMode = mode;
        setLongTaskExecutor();
    }

    protected void setScheduledExecutionMode(
                    TestSynchronousExecutor.Mode mode) {
        scheduledExecutionMode = mode;
        setScheduledExecutor();
    }

    protected <T extends Message> void setPnfsMessageProcessor(
                    TestMessageProcessor<T> processor) {
        testPnfsManagerStub.setProcessor(processor);
    }

    protected void setPnfsNotifyProcessor(TestNotifyProcessor processor) {
        testPnfsManagerStub.setNotifyProcessor(processor);
    }

    protected <T extends Message> void setPoolMessageProcessor(
                    TestMessageProcessor<T> processor) {
        testPoolStub.setProcessor(processor);
    }

    protected void setShortExecutionMode(TestSynchronousExecutor.Mode mode) {
        shortTaskExecutionMode = mode;
        setShortTaskExecutor();
    }

    protected void setUpBase() throws CacheException {
        createAccess();
        createCellStubs();
        createCellStubFactory();
        createCostModule();
        createSelectionUnit();
        createPoolMonitor();
        createPoolInfoMap();

        wireCellStubFactory();
        wirePoolMonitor();
        wirePoolInfoMap();

        initializeCostModule();
        initializeSelectionUnit();
        initializePoolInfoMap();

        /*
         * Leave out other initializations here; taken care of in
         * the specific test case.
         */
    }

    protected void wireCellStubFactory() {
        cellStubFactory.setPoolManager(mock(CellStub.class));
    }

    protected void wirePnfsOperationHandler() {
        pnfsTaskCompletionHandler.setMap(pnfsOperationMap);
        pnfsOperationHandler.setCompletionHandler(pnfsTaskCompletionHandler);
        pnfsOperationHandler.setFactory(cellStubFactory);
        pnfsOperationHandler.setInaccessibleFileHandler(
                        inaccessibleFileHandler);
        pnfsOperationHandler.setNamespace(testNamespaceAccess);
        pnfsOperationHandler.setPoolInfoMap(poolInfoMap);
        pnfsOperationHandler.setPnfsOpMap(pnfsOperationMap);
        pnfsOperationHandler.setTaskService(longJobExecutor);
        pnfsOperationHandler.setScheduledService(scheduledExecutorService);
    }

    protected void wirePnfsOperationMap() {
        pnfsOperationMap.setCompletionHandler(pnfsTaskCompletionHandler);
        pnfsOperationMap.setPoolTaskCompletionHandler(poolTaskCompletionHandler);
        pnfsOperationMap.setCounters(counters);
        OperationHistory  history = new OperationHistory();
        history.setCapacity(1);
        history.initialize();
        pnfsOperationMap.setHistory(history);
        pnfsOperationMap.setCopyThreads(2);
        pnfsOperationMap.setMaxRetries(2);
        pnfsOperationMap.setOperationHandler(pnfsOperationHandler);
        pnfsOperationMap.setPoolInfoMap(poolInfoMap);
        pnfsOperationMap.setCheckpointExpiry(Long.MAX_VALUE);
        pnfsOperationMap.setCheckpointExpiryUnit(TimeUnit.MILLISECONDS);
        pnfsOperationMap.setCheckpointFilePath(CHKPTFILE);
    }

    protected void wirePoolInfoMap() {
        poolInfoMap.setCacheExpiry(Long.MAX_VALUE);
        poolInfoMap.setCacheExpiryUnit(TimeUnit.DAYS);
        poolInfoMap.setPoolSelectionStrategy(
                        new ProportionalPoolSelectionStrategy());
        poolInfoMap.setCellStubFactory(cellStubFactory);
    }

    protected void wirePoolMonitor() {
        testPoolMonitor.setCostModule(testCostModule);
        testPoolMonitor.setSelectionUnit(testSelectionUnit);
    }

    protected void wirePoolOperationHandler() {
        poolTaskCompletionHandler.setMap(poolOperationMap);
        poolOperationHandler.setCompletionHandler(poolTaskCompletionHandler);
        poolOperationHandler.setNamespace(testNamespaceAccess);
        poolOperationHandler.setOperationMap(poolOperationMap);
        poolOperationHandler.setScanService(longJobExecutor);
        poolOperationHandler.setSubmitService(shortJobExecutor);
    }

    protected void wirePoolOperationMap() {
        poolOperationMap.setDownGracePeriod(0);
        poolOperationMap.setDownGracePeriodUnit(TimeUnit.MINUTES);
        poolOperationMap.setHandleRestarts(true); // override default for testing
        poolOperationMap.setMaxConcurrentRunning(2);
        poolOperationMap.setRescanWindow(0);
        poolOperationMap.setRescanWindowUnit(TimeUnit.HOURS);
        poolOperationMap.setCounters(counters);
        poolOperationMap.setPoolInfoMap(poolInfoMap);
        poolOperationMap.setHandler(poolOperationHandler);
    }

    protected void wirePsuDecorator() {
        decorator.setDelegate(testSelectionUnit.psu);
        decorator.setPsu(testSelectionUnit);
        decorator.setService(shortJobExecutor);
        decorator.setResilienceTopic(testPnfsManagerStub);
        decorator.setCostModule(testCostModule);
    }
}
