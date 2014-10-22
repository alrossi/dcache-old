package org.dcache.replication.v3;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.dcache.util.CDCScheduledExecutorServiceDecorator;

/**
 * @author arossi
 */
public final class CDCScheduledPoolTaskExecutor
                    extends CDCTaskExecutor<CDCScheduledExecutorServiceDecorator>
                    implements ScheduledExecutorService {

    public void initialize() {
        ScheduledExecutorService executor
            = Executors.newScheduledThreadPool(numberOfThreads);
        delegate = new CDCScheduledExecutorServiceDecorator(executor);
    }

    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay,
                    TimeUnit unit) {
        return delegate.schedule(callable, delay, unit);
    }

    public ScheduledFuture<?> schedule(Runnable command, long delay,
                    TimeUnit unit) {
        return delegate.schedule(command, delay, unit);
    }

    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command,
                    long initialDelay, long period, TimeUnit unit) {
        return delegate.scheduleAtFixedRate(command, initialDelay, period, unit);
    }

    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command,
                    long initialDelay, long delay, TimeUnit unit) {
        return delegate.scheduleWithFixedDelay(command, initialDelay, delay, unit);
    }
}
