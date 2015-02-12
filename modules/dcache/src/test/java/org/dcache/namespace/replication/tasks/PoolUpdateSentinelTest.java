package org.dcache.namespace.replication.tasks;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import diskCacheV111.vehicles.PoolStatusChangedMessage;
import org.dcache.namespace.replication.ReplicationHub;
import org.dcache.namespace.replication.caches.PoolStatusCache;
import org.dcache.namespace.replication.data.PoolStatusMessageType;
import org.dcache.namespace.replication.monitoring.ActivityRegistry;
import org.dcache.namespace.replication.tasks.PoolUpdateSentinel.State;

/**
 * Created by arossi on 2/10/15.
 */
public class PoolUpdateSentinelTest {

    /*
     *  Test subclass does no actual work.
     */
    class TestSentinel extends PoolUpdateSentinel {
        TestSentinel(ReplicaTaskInfo info, ReplicationHub hub) {
            super(info, hub);
        }

        @Override
        public void launchProcessPool() {
            /*
             * to avoid constructing the real task and submitting it to
             * to the executor service.
             */
        }
    }

    ActivityRegistry registry;
    PoolStatusCache cache;
    ReplicationHub hub;
    ReplicaTaskInfo info;

    TestSentinel sentinel;

    @Before
    public void setUp() {
        registry = new ActivityRegistry();
        cache = new PoolStatusCache();
        cache.setLifetime(1);
        cache.setLifetimeUnit(TimeUnit.SECONDS);
        cache.setSize(5);
        cache.initialize();
        hub = new ReplicationHub();
        hub.setPoolStatusCache(cache);
        hub.setRegistry(registry);
    }

    @After
    public void tearDown() throws Exception {
        sentinel.done();
        waitFor(1);
        verifyCacheUnregisteredPool();
    }

    @Test
    public void shouldRegisterWhenStarted() {
        givenHubHasGracePeriod(0);
        givenInitialStatusWasPoolDown();
        whenSentinelHasStarted();
        verifyInfoSetSentinel();
        verifyCacheRegisteredPool();
    }

    @Test
    public void shouldResetRestartWaitToDownWaitAfterDown() {
        givenHubHasGracePeriod(30);
        givenInitialStatusWasPoolRestart();
        whenSentinelHasStarted();
        whenPoolStatusChangesToDown();
        verifySentinelCurrentIs(State.DOWN_WAIT);
    }

    @Test
    public void shouldNotResetRestartWaitAfterRestart() {
        givenHubHasGracePeriod(30);
        givenInitialStatusWasPoolRestart();
        whenSentinelHasStarted();
        whenPoolStatusChangesToUp();
        verifySentinelCurrentIs(State.RESTART_WAIT);
        whenPoolStatusChangesToRestart();
        verifySentinelCurrentIs(State.RESTART_WAIT);
    }

    @Test
    public void shouldCancelAndSetNextToDownWaitAfterDownWhileRestartRunning()
                    throws InterruptedException {
        givenHubHasGracePeriod(5);
        givenInitialStatusWasPoolRestart();
        whenSentinelHasStarted();
        whenPoolStatusChangesToDown();
        verifySentinelNextIs(State.DOWN_WAIT);
        sentinel.done();
//        whenInfoIsDone();
//        verifyInfoCancel();
    }

    @Test
    public void shouldNotSetNextAfterRestartWhileRestartRunning() {
        givenHubHasGracePeriod(30);
        givenInitialStatusWasPoolRestart();
        whenSentinelHasStarted();
        whenPoolStatusChangesToUp();
        verifySentinelNextIs(null);
        whenPoolStatusChangesToRestart();
        verifySentinelNextIs(null);
    }

    @Test
    public void shouldNotResetDownWaitAfterDown() {
        givenHubHasGracePeriod(30);
        givenInitialStatusWasPoolDown();
        whenSentinelHasStarted();
        whenPoolStatusChangesToDown();
        verifySentinelCurrentIs(State.DOWN_WAIT);
    }

    @Test
    public void shouldCancelDownWaitAfterRestart() throws InterruptedException {
        givenHubHasGracePeriod(30);
        givenInitialStatusWasPoolDown();
        whenSentinelHasStarted();
        whenPoolStatusChangesToUp();
        verifySentinelNextIs(null);
//        info.replicaTaskInfoFuture.get();
//        verifyInfoCancel();
    }

    @Test
    public void shouldNotSetNextWhileDownRunningAfterDown() {
        givenHubHasGracePeriod(30);
        givenInitialStatusWasPoolDown();
        whenSentinelHasStarted();
        whenPoolStatusChangesToDown();
        verifySentinelNextIs(null);
    }

    @Test
    public void shouldSetNextToRestartWaitAfterRestartWhileDownRunning() {
        givenHubHasGracePeriod(30);
        givenInitialStatusWasPoolDown();
        whenSentinelHasStarted();
        whenPoolStatusChangesToUp();
        verifySentinelNextIs(State.RESTART_WAIT);
    }

    private void givenHubHasGracePeriod(long grace) {
        hub.setPoolStatusGracePeriod(grace);
        hub.setPoolStatusGracePeriodUnit(TimeUnit.SECONDS);
    }

    private void givenInitialStatusWasPoolDown() {
        info = new ReplicaTaskInfo("testPool", PoolStatusMessageType.DOWN);
    }

    private void givenInitialStatusWasPoolRestart() {
        info = new ReplicaTaskInfo("testPool", PoolStatusMessageType.RESTART);
    }

    private void verifyInfoSetSentinel() {
        if (!info.hasSentinel()) {
            throw new AssertionError("info.hasSentinel()");
        }
    }

    private void verifyInfoCancel() {
        if (!info.isCancelled()) {
            throw new AssertionError("info.isCancelled()");
        }
    }

    private void verifyCacheRegisteredPool() {
        if (!cache.isRegistered("testPool")) {
            throw new AssertionError("pool registered");
        }
    }

    private void verifyCacheUnregisteredPool() {
        if (cache.isRegistered("testPool")) {
            throw new AssertionError("pool unregistered");
        }
    }

    private void verifySentinelCurrentIs(State state) {
        synchronized(sentinel) {
            assert(sentinel.current == state);
        }
    }

    private void verifySentinelNextIs(State state) {
        synchronized(sentinel) {
            assert(sentinel.next == state);
        }
    }

    private void waitFor(int sec) {
        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(sec));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void whenInfoIsDone() throws InterruptedException {
        info.replicaTaskInfoFuture.get();
    }

    private void whenPoolStatusChangesToDown() {
        if (sentinel != null) {
            sentinel.messageArrived(new PoolStatusChangedMessage("testPool",
                                        PoolStatusChangedMessage.DOWN));
        }
    }

    private void whenPoolStatusChangesToUp() {
        if (sentinel != null) {
            sentinel.messageArrived(new PoolStatusChangedMessage("testPool",
                                        PoolStatusChangedMessage.UP));
        }
    }

    private void whenPoolStatusChangesToRestart() {
        if (sentinel != null) {
            sentinel.messageArrived(new PoolStatusChangedMessage("testPool",
                                        PoolStatusChangedMessage.RESTART));
        }
    }

    private void whenSentinelHasStarted() {
        sentinel = new TestSentinel(info, hub);
        sentinel.start();
    }
}
