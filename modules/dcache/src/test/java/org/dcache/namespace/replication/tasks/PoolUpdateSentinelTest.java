package org.dcache.namespace.replication.tasks;

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

    class TestSentinel extends PoolUpdateSentinel {
        boolean launched = false;

        TestSentinel(ReplicaTaskInfo info, ReplicationHub hub) {
            super(info, hub);
        }

        @Override
        public void launchProcessPool() {
            launched = true;
        }
    }

    ReplicationHub hub = mock(ReplicationHub.class);
    ReplicaTaskInfo info = mock(ReplicaTaskInfo.class);
    PoolStatusCache cache = mock(PoolStatusCache.class);

    TestSentinel sentinel;

    @Test
    public void shouldRestToDownWhenRestartWaitIsInterruptedByDown() {
        given(hub.getPoolStatusGracePeriod()).willReturn(1L);
        given(hub.getPoolStatusGracePeriodUnit()).willReturn(TimeUnit.MINUTES);
        given(hub.getPoolStatusCache()).willReturn(cache);
        given(info.getName()).willReturn("test");
        given(info.getType()).willReturn(Type.POOL_RESTART);
        given(info.isDone()).willReturn(false);

        sentinel = new TestSentinel(info, hub);

        PoolStatusChangedMessage msg
                        = new PoolStatusChangedMessage("test", PoolStatusChangedMessage.DOWN);

        sentinel.start();

        verify(info).setSentinel(sentinel);
        verify(cache).registerPoolSentinel(sentinel);

        sentinel.messageArrived(msg);
        assert(sentinel.current == State.DOWN_WAIT);
        sentinel.current = State.DOWN_RUNNING;

        sentinel.done();

    }
}
