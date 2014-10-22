package org.dcache.replication.v3;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author arossi
 *
 */
public abstract class CDCTaskExecutor<E extends ExecutorService> implements
                ExecutorService {
    protected E delegate;
    protected int numberOfThreads;

    public boolean awaitTermination(long timeout, TimeUnit unit)
                    throws InterruptedException {
        return delegate.awaitTermination(timeout, unit);
    }

    public void execute(Runnable command) {
        delegate.execute(command);
    }

    public abstract void initialize();

    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
                    throws InterruptedException {
        return delegate.invokeAll(tasks);
    }

    public <T> List<Future<T>> invokeAll(
                    Collection<? extends Callable<T>> tasks, long timeout,
                    TimeUnit unit) throws InterruptedException {
        return delegate.invokeAll(tasks, timeout, unit);
    }

    public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
                    throws InterruptedException, ExecutionException {
        return delegate.invokeAny(tasks);
    }

    public <T> T invokeAny(Collection<? extends Callable<T>> tasks,
                    long timeout, TimeUnit unit) throws InterruptedException,
                    ExecutionException, TimeoutException {
        return delegate.invokeAny(tasks, timeout, unit);
    }

    public boolean isShutdown() {
        return delegate.isShutdown();
    }

    public boolean isTerminated() {
        return delegate.isTerminated();
    }

    public void setNumberOfThreads(int numberOfThreads) {
        this.numberOfThreads = numberOfThreads;
    }

    public void shutdown() {
        delegate.shutdown();
    }

    public List<Runnable> shutdownNow() {
        return delegate.shutdownNow();
    }

    public <T> Future<T> submit(Callable<T> task) {
        return delegate.submit(task);
    }

    public Future<?> submit(Runnable task) {
        return delegate.submit(task);
    }

    public <T> Future<T> submit(Runnable task, T result) {
        return delegate.submit(task, result);
    }
}
