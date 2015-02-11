package org.dcache.namespace.replication.tasks;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import diskCacheV111.vehicles.PoolStatusChangedMessage;
import org.dcache.namespace.replication.ReplicationHub;
import org.dcache.namespace.replication.caches.PoolStatusCache;
import org.dcache.namespace.replication.tasks.PoolUpdateSentinel.State;
import org.dcache.namespace.replication.tasks.ReplicaTaskInfo.Type;

import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

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
            // to avoid constructing real task and submitting to executor
        }
    }

    ReplicationHub hub;
    ReplicaTaskInfo info;
    PoolStatusCache cache;
    TestSentinel sentinel;

    @Before
    public void setUp() {
        hub = mock(ReplicationHub.class);
        info = mock(ReplicaTaskInfo.class);
        cache = mock(PoolStatusCache.class);
    }

    @After
    public void tearDown() {
        whenSentinelHasCompleted();
        verify(cache).unregisterPoolSentinel(sentinel);
    }

    @Test
    public void shouldRegisterWhenStarted() {
        givenHubHasGracePeriod(30);
        givenInitialStatusWasPoolDown();
        whenSentinelHasStarted();
        verify(info).setSentinel(sentinel);
        verify(cache).registerPoolSentinel(sentinel);
    }

    @Test
    public void shouldResetRestartWaitToDownWaitAfterDown() {
        givenHubHasGracePeriod(30);
        givenInitialStatusWasPoolRestart();
        whenSentinelHasStarted();
        whenPoolStatusChangesToDown();
        assert(sentinel.current == State.DOWN_WAIT);
    }

    @Test
    public void shouldNotResetRestartWaitAfterRestart() {
        givenHubHasGracePeriod(30);
        givenInitialStatusWasPoolRestart();
        whenSentinelHasStarted();
        whenPoolStatusChangesToUp();
        assert(sentinel.current == State.RESTART_WAIT);
        whenPoolStatusChangesToRestart();
        assert(sentinel.current == State.RESTART_WAIT);
    }

    @Test
    public void shouldCancelAndSetNextToDownWaitAfterDownWhileRestartRunning() {
        givenHubHasGracePeriod(30);
        givenInitialStatusWasPoolRestart();
        whenSentinelHasStarted();
        whenProcessPoolStartsToRun();
        whenPoolStatusChangesToDown();
        assert(sentinel.next == State.DOWN_WAIT);
        verify(info).cancel();
    }

    @Test
    public void shouldNotSetNextAfterRestartWhileRestartRunning() {
        givenHubHasGracePeriod(30);
        givenInitialStatusWasPoolRestart();
        whenSentinelHasStarted();
        whenProcessPoolStartsToRun();
        whenPoolStatusChangesToUp();
        assert(sentinel.next == null);
        whenPoolStatusChangesToRestart();
        assert(sentinel.next == null);
    }

    @Test
    public void shouldNotResetDownWaitAfterDown() {
        givenHubHasGracePeriod(30);
        givenInitialStatusWasPoolDown();
        whenSentinelHasStarted();
        whenPoolStatusChangesToDown();
        assert(sentinel.current == State.DOWN_WAIT);
    }

    @Test
    public void shouldCancelDownWaitAfterRestart() {
        givenHubHasGracePeriod(30);
        givenInitialStatusWasPoolDown();
        whenSentinelHasStarted();
        whenPoolStatusChangesToUp();
        verify(info).cancel();
    }

    @Test
    public void shouldNotSetNextWhileDownRunningAfterDown() {
        givenHubHasGracePeriod(30);
        givenInitialStatusWasPoolDown();
        whenSentinelHasStarted();
        whenProcessPoolStartsToRun();
        whenPoolStatusChangesToDown();
        assert(sentinel.next == null);
    }

    @Test
    public void shouldSetNextToRestartWaitAfterRestartWhileDownRunning() {
        givenHubHasGracePeriod(30);
        givenInitialStatusWasPoolDown();
        whenSentinelHasStarted();
        whenProcessPoolStartsToRun();
        whenPoolStatusChangesToUp();
        assert(sentinel.next == State.RESTART_WAIT);
    }

    private void givenHubHasGracePeriod(long grace) {
        given(hub.getPoolStatusGracePeriod()).willReturn(grace);
        given(hub.getPoolStatusGracePeriodUnit()).willReturn(TimeUnit.SECONDS);
        given(hub.getPoolStatusCache()).willReturn(cache);
    }

    private void givenInitialStatusWasPoolDown() {
        given(info.getName()).willReturn("test");
        given(info.getType()).willReturn(Type.POOL_DOWN);
    }

    private void givenInitialStatusWasPoolRestart() {
        given(info.getName()).willReturn("test");
        given(info.getType()).willReturn(Type.POOL_RESTART);
    }

    private void whenPoolStatusChangesToDown() {
        if (sentinel != null) {
            sentinel.messageArrived(new PoolStatusChangedMessage("test",
                                        PoolStatusChangedMessage.DOWN));
        }
    }

    private void whenPoolStatusChangesToUp() {
        if (sentinel != null) {
            sentinel.messageArrived(new PoolStatusChangedMessage("test",
                                        PoolStatusChangedMessage.UP));
        }
    }

    private void whenPoolStatusChangesToRestart() {
        if (sentinel != null) {
            sentinel.messageArrived(new PoolStatusChangedMessage("test",
                                        PoolStatusChangedMessage.RESTART));
        }
    }

    private void whenProcessPoolStartsToRun() {
        if (sentinel != null) {
            switch (sentinel.current) {
                case DOWN_WAIT:
                    sentinel.current = State.DOWN_RUNNING;
                    break;
                case RESTART_WAIT:
                    sentinel.current = State.RESTART_RUNNING;
                    break;
            }
            sentinel.launchProcessPool();
        }
    }

    private void whenSentinelHasCompleted() {
        if (sentinel != null) {
            sentinel.done();
        }
    }

    private void whenSentinelHasStarted() {
        sentinel = new TestSentinel(info, hub);
        sentinel.start();
    }
}
