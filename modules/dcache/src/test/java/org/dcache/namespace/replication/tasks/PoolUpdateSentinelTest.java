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
    private static final int DELAY_UNTIL_RUNNING = 200;
    private static final int NO_DELAY_UNTIL_RUNNING = 0;

    /*
     * To avoid constructing the real task and submitting it to
     * to the executor service.
     */
    class TestSentinel extends PoolUpdateSentinel {
        TestSentinel(ReplicaTaskInfo info, ReplicationHub hub) {
            super(info, hub);
        }

        @Override
        protected long launchProcessPool() {
            LOGGER.debug("Calling launch process pool.");

            switch(current) {
                case DOWN_WAIT:
                    current = State.DOWN_RUNNING; break;
                case RESTART_WAIT:
                    current = State.RESTART_RUNNING; break;
                default:
                    throw new IllegalStateException(String.format("Should not be "
                                    + "launching process pool from state %s; "
                                    + "this is a bug.", current));
            }

            task.info = TestSentinel.this.info;
            new Thread(task).start();

            return Long.MAX_VALUE;
        }
    }

    class DummyProcessPool implements Runnable {
        ReplicaTaskInfo info;

        @Override
        public void run() {
            /*
             * The behavior of the pool processing task is to
             * call done on the entire operation when it completes.
             * For the purposes of the logic of the tests, we need
             * to guarantee the proper sequence of arrival of messages
             * in order to be assured that we are verifying the correct
             * conditions upon exit of this thread, so it waits
             * for notification from the testing thread.
             */
            try {
                if (taskShouldWait) {
                    synchronized(DummyProcessPool.this) {
                        wait();
                    }
                }
            } catch (InterruptedException e) {}

            info.done();
        }
    }

    ActivityRegistry registry;
    PoolStatusCache cache;
    ReplicationHub hub;
    ReplicaTaskInfo info;
    TestSentinel sentinel;
    DummyProcessPool task;

    boolean taskShouldWait;

    @Before
    public void setUp() {
        registry = new ActivityRegistry();
        cache = new PoolStatusCache();
        cache.setLifetime(DELAY_UNTIL_RUNNING);
        cache.setLifetimeUnit(TimeUnit.SECONDS);
        cache.setSize(1);
        cache.initialize();
        hub = new ReplicationHub();
        hub.setPoolStatusCache(cache);
        hub.setRegistry(registry);
        task = new DummyProcessPool();
        taskShouldWait = false;
    }

    @After
    public void tearDown() throws InterruptedException {
        verifySentinelInfoCompleted();
    }

    @Test
    public void shouldRegisterWhenStarted() {
        givenHubHasGracePeriod(NO_DELAY_UNTIL_RUNNING);
        givenInitialStatusWasPoolDown();
        whenSentinelHasStarted();
        verifyInfoSetSentinel();
        verifyCacheRegisteredPool();
    }

    @Test
    public void shouldResetRestartWaitToDownWaitAfterDown() {
        givenHubHasGracePeriod(DELAY_UNTIL_RUNNING);
        givenInitialStatusWasPoolRestart();
        whenSentinelHasStarted();
        whenPoolStatusChangesToDown();
        verifySentinelCurrentIs(State.DOWN_WAIT);
    }

    @Test
    public void shouldNotResetRestartWaitAfterUp() {
        givenHubHasGracePeriod(DELAY_UNTIL_RUNNING);
        givenInitialStatusWasPoolRestart();
        whenSentinelHasStarted();
        whenPoolStatusChangesToUp();
        verifySentinelCurrentIs(State.RESTART_WAIT);
    }

    @Test
    public void shouldCancelAndSetNextToDownWaitAfterDownWhileRestartRunning()
                    throws InterruptedException {
        givenHubHasGracePeriod(NO_DELAY_UNTIL_RUNNING);
        givenProcessTaskShouldFinishAfterMessageIsReceived();
        givenInitialStatusWasPoolRestart();
        whenSentinelHasStarted();
        whenTaskBeginsToRun();
        whenPoolStatusChangesToDown();
        verifySentinelNextIs(State.DOWN_WAIT);
        /*
         * We should not try to verify that info is cancelled
         * because the task may call info done first.
         * It is sufficient to know info has completed.
         * This is the original info.  The internal info
         * is actually reset by this sequence, and is checked
         * as usual in the @After method.
         */
        verifyInfoCompleted();
    }

    @Test
    public void shouldNotSetNextAfterUpWhileRestartRunning() {
        givenHubHasGracePeriod(NO_DELAY_UNTIL_RUNNING);
        givenProcessTaskShouldFinishAfterMessageIsReceived();
        givenInitialStatusWasPoolRestart();
        whenSentinelHasStarted();
        whenTaskBeginsToRun();
        whenPoolStatusChangesToUp();
        verifySentinelNextIs(null);
    }

    @Test
    public void shouldNotResetDownWaitAfterDown() {
        givenHubHasGracePeriod(DELAY_UNTIL_RUNNING);
        givenInitialStatusWasPoolDown();
        whenSentinelHasStarted();
        whenPoolStatusChangesToDown();
        verifySentinelCurrentIs(State.DOWN_WAIT);
    }

    @Test
    public void shouldNotResetDownRunningAfterDown() {
        givenHubHasGracePeriod(DELAY_UNTIL_RUNNING);
        givenProcessTaskShouldFinishAfterMessageIsReceived();
        givenInitialStatusWasPoolDown();
        whenSentinelHasStarted();
        whenTaskBeginsToRun();
        whenPoolStatusChangesToDown();
        verifySentinelCurrentIs(State.DOWN_WAIT);
    }

    @Test
    public void shouldCancelDownWaitAfterUp() throws InterruptedException {
        givenHubHasGracePeriod(DELAY_UNTIL_RUNNING);
        givenInitialStatusWasPoolDown();
        whenSentinelHasStarted();
        whenPoolStatusChangesToUp();
        verifySentinelNextIs(null);
        verifyInfoCancel();
    }

    @Test
    public void shouldNotSetNextWhileDownRunningAfterDown() {
        givenHubHasGracePeriod(NO_DELAY_UNTIL_RUNNING);
        givenProcessTaskShouldFinishAfterMessageIsReceived();
        givenInitialStatusWasPoolDown();
        whenSentinelHasStarted();
        whenTaskBeginsToRun();
        whenPoolStatusChangesToDown();
        verifySentinelNextIs(null);
    }

    @Test
    public void shouldSetNextToRestartWaitAfterUpWhileDownRunning() {
        givenHubHasGracePeriod(NO_DELAY_UNTIL_RUNNING);
        givenProcessTaskShouldFinishAfterMessageIsReceived();
        givenInitialStatusWasPoolDown();
        whenSentinelHasStarted();
        whenTaskBeginsToRun();
        whenPoolStatusChangesToRestart();
        verifySentinelNextIs(State.RESTART_WAIT);
    }

    private void givenHubHasGracePeriod(long grace) {
        hub.setPoolStatusGracePeriod(grace);
        hub.setPoolStatusGracePeriodUnit(TimeUnit.MILLISECONDS);
    }

    private void givenInitialStatusWasPoolDown() {
        info = new ReplicaTaskInfo("testPool", PoolStatusMessageType.DOWN);
    }

    private void givenInitialStatusWasPoolRestart() {
        info = new ReplicaTaskInfo("testPool", PoolStatusMessageType.RESTART);
    }

    private void givenProcessTaskShouldFinishAfterMessageIsReceived() {
        taskShouldWait = true;
    }

    private void verifyInfoSetSentinel() {
        if (!info.hasSentinel()) {
            throw new AssertionError("info.hasSentinel()");
        }
    }

    private void verifyInfoCancel() throws InterruptedException {
        info.replicaTaskInfoFuture.get();
        if (!info.isCancelled()) {
            throw new AssertionError("info.isCancelled()");
        }
    }

    private void verifyInfoCompleted() throws InterruptedException {
        info.replicaTaskInfoFuture.get();
        if (!info.isDone()) {
            throw new AssertionError("info.isDone()");
        }
    }

    private void verifyCacheRegisteredPool() {
        if (!cache.isRegistered("testPool")) {
            throw new AssertionError("pool registered");
        }
    }

    private void verifySentinelCurrentIs(State state) {
        synchronized(sentinel) {
            assert(sentinel.current == state);
        }
    }

    private void verifySentinelInfoCompleted() throws InterruptedException {
        sentinel.info.replicaTaskInfoFuture.get();
        if (!sentinel.info.isDone()) {
            throw new AssertionError("sentinel.info.isDone()");
        }
    }

    private void verifySentinelNextIs(State state) {
        synchronized(sentinel) {
            assert(sentinel.next == state);
        }
    }

    private void whenPoolStatusChangesToDown() {
        if (sentinel != null) {
            sentinel.messageArrived(new PoolStatusChangedMessage("testPool",
                                        PoolStatusChangedMessage.DOWN));
        }

        synchronized (task) {
            task.notifyAll();
        }
    }

    private void whenPoolStatusChangesToUp() {
        if (sentinel != null) {
            sentinel.messageArrived(new PoolStatusChangedMessage("testPool",
                                        PoolStatusChangedMessage.UP));
        }

        synchronized (task) {
            task.notifyAll();
        }
    }

    private void whenPoolStatusChangesToRestart() {
        if (sentinel != null) {
            sentinel.messageArrived(new PoolStatusChangedMessage("testPool",
                                        PoolStatusChangedMessage.RESTART));
        }

        synchronized (task) {
            task.notifyAll();
        }

    }

    private void whenSentinelHasStarted() {
        sentinel = new TestSentinel(info, hub);
        sentinel.start();
    }

    private void whenTaskBeginsToRun() {
        synchronized(sentinel) {
            while (sentinel.current == State.DOWN_WAIT ||
                   sentinel.current == State.RESTART_WAIT) {
                try {
                    sentinel.wait(100);
                } catch (InterruptedException e) {
                }
            }
        }
    }
}
