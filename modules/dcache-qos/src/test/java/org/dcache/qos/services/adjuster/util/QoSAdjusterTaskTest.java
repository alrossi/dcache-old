package org.dcache.qos.services.adjuster.util;

import static org.dcache.mock.PoolManagerInformationBuilder.poolManagerInformation;
import static org.dcache.mock.PoolMigrationCopyReplicaMessageBuilder.aPoolMigrationCopyReplicaMessage;
import static org.dcache.mock.QoSAdjusterTaskBuilder.anAdjusterTask;
import static org.dcache.mock.QoSAdjustmentRequestBuilder.anAdjustmentRequest;
import static org.dcache.qos.data.QoSAction.COPY_REPLICA;
import static org.dcache.qos.services.scanner.handlers.PoolOpChangeHandlerTest.DEFAULT_POOL_INFO;
import static org.dcache.qos.util.MessageGuardTest.TEST_PNFSID;
import static org.dcache.util.FileAttributesBuilder.fileAttributes;
import static org.dcache.util.StorageInfoBuilder.aStorageInfo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.Futures;
import diskCacheV111.poolManager.CostModule;
import dmg.cells.nucleus.CellPath;
import java.util.concurrent.Executors;
import org.dcache.cells.CellStub;
import org.dcache.mock.CostModuleFactory;
import org.dcache.pool.migration.PoolMigrationCopyReplicaMessage;
import org.dcache.qos.services.adjuster.adjusters.QoSAdjusterFactory;
import org.dcache.qos.services.adjuster.handlers.QoSAdjustTaskCompletionHandler;
import org.junit.Before;
import org.junit.Test;

public class QoSAdjusterTaskTest {

    QoSAdjusterFactory factory;
    CostModule costModule;
    CellStub pool;

    QoSAdjusterTask task;

    @Before
    public void setup() {
        factory = new QoSAdjusterFactory();
        factory.setScheduledExecutor(Executors.newSingleThreadScheduledExecutor());
        factory.setCompletionHandler(mock(QoSAdjustTaskCompletionHandler.class));
        factory.setPinManager(mock(CellStub.class));
        pool = mock(CellStub.class);
        when(pool.send(any(CellPath.class), any(PoolMigrationCopyReplicaMessage.class))).thenReturn(
              Futures.immediateFuture(
                    aPoolMigrationCopyReplicaMessage().build()));
        factory.setPools(pool);
        costModule = CostModuleFactory.create(DEFAULT_POOL_INFO);
    }

    @Test
    public void tryIt() {
        given(anAdjusterTask().usingFactory(factory)
              .from(anAdjustmentRequest().withAction(COPY_REPLICA)
                    .withPnfsId(TEST_PNFSID)
                    .withAttributes(fileAttributes()
                          .withId(TEST_PNFSID)
                          .withAtime(System.currentTimeMillis())
                          .withLocations("testpool03-3")
                          .withStorageInfo(
                                aStorageInfo().withHsm("enstore").withLocation("testpool03-3")))
                    .withPoolGroup("persistent-group")
                    .withSource("testpool03-3")
                    .withTarget("testpool09-4")
                    .withTargetInfo(poolManagerInformation().withPool("testpool03-3")
                          .withCpuCost(0.1).withCostModule(costModule))));
        whenTaskIsRun();
    }

    private void given(QoSAdjusterTask task) {
        this.task = task;
    }

    private void whenTaskIsRun() {
        task.run();
    }
}
