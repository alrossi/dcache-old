package org.dcache.replication.v3;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.dcache.util.CDCExecutorServiceDecorator;

/**
 * @author arossi
 */
public final class CDCFixedPoolTaskExecutor
    extends CDCTaskExecutor<CDCExecutorServiceDecorator> {

    public void initialize() {
        ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
        delegate = new CDCExecutorServiceDecorator(executor);
    }
}
