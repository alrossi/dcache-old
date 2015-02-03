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
package org.dcache.namespace.replication;

import com.google.common.base.Preconditions;

import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import diskCacheV111.util.CacheException;
import org.dcache.namespace.replication.workers.PoolScanWorker;
import org.dcache.util.replication.RunnableModule;

/**
 * Component which is responsible for running a periodic health check on
 * the resilient pools to make sure that replicas are properly constrained
 * between min and max.
 * <p/>
 * The period of this scan should probably not be any less than 24 hours.
 *
 * Created by arossi on 1/25/15.
 */
public final class ResilienceWatchdog extends RunnableModule {
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean scanning = new AtomicBoolean(false);
    private final AtomicLong nextCheck = new AtomicLong(System.currentTimeMillis());

    private ReplicaManagerHub hub;
    private ReplicationMessageHandler handler;
    private Future<String> workerFuture;

    public void cancel() {
        if (!scanning.getAndSet(false)) {
            return;
        }
        synchronized (this) {
            if (workerFuture != null) {
                workerFuture.cancel(true);
            }
        }
    }

    public long getNextScan() {
        return nextCheck.get();
    }

    public void initialize() {
        Preconditions.checkNotNull(hub);
        Preconditions.checkNotNull(handler);
        super.initialize();
    }

    public boolean isRunning() {
        return running.get();
    }

    public void run() {
        running.set(true);

        long waitInMs = timeoutUnit.toMillis(timeout);

        while (running.get()) {
            if (System.currentTimeMillis() >= nextCheck.get()) {
                scanning.set(true);
                doScan();
                scanning.set(false);
                nextCheck.set(System.currentTimeMillis() + waitInMs);
            }

            try {
                Thread.sleep(nextCheck.get() - System.currentTimeMillis());
            } catch (InterruptedException ie) {
                LOGGER.warn("Thread interrupted, running check now ...");
            }
        }
    }

    public void reschedule(long time, TimeUnit timeUnit) {
        nextCheck.set(timeUnit.toMillis(time));
        threadInterrupt();
    }

    public void setHandler(ReplicationMessageHandler handler) {
        this.handler = handler;
    }

    public void setHub(ReplicaManagerHub hub) {
        this.hub = hub;
    }

    public void shutdown() {
        running.set(false);
        super.shutdown();
    }

    private void doScan() {
        Date date = new Date(nextCheck.get());
        LOGGER.info("Periodic resilient pool scan, scheduled for {}, "
                                        + "has started.", date);

        Collection<String> activePools = null;
        Collection<String> seenPnfsids = new HashSet<>();

        try {
            activePools = hub.getPoolInfoCache().findAllActivePools();
        } catch (CacheException e) {
            LOGGER.error("A problem occurred while trying to get active pools: {}.",
                            e.getMessage());
            LOGGER.debug("scan(): pool selection unit problem", e);
            return;
        } catch (InterruptedException ie) {
            LOGGER.warn("Thread interrupted during watchdog scan, "
                            + "which cannot be completed at this time.");
            return;
        }

        /**
         * Executes sequentially.
         */
        for (String pool : activePools) {
            if (!scanning.get()) {
                break;
            }

            LOGGER.debug("scan(): checking pool {}.", pool);
            if (hub.getPoolStatusCache().isRegistered(pool)) {
                /*
                 * There is a currently active update going on for this
                 * pool, so just leave it alone.
                 */
                LOGGER.debug("Pool {} is currently being handled by "
                                + "an update worker.", pool);
                continue;
            }

            synchronized(this) {
                PoolScanWorker worker = new PoolScanWorker(pool, seenPnfsids, hub);

                LOGGER.debug("Starting worker to scan {}.", pool);

                worker.run();
                workerFuture = worker.getWorkerFuture();

                try {
                    workerFuture.get();
                } catch (InterruptedException | ExecutionException e) {
                    LOGGER.error("A problem occurred while waiting for scan of "
                                                    + "pool {} to finish: {}.",
                                    pool, e.getMessage());
                }

                LOGGER.debug("Worker to scan {} completed.", pool);

                workerFuture = null;
            }
        }

        LOGGER.info("Periodic resilient pool scan, scheduled for {}, "
                        + "has completed.", date);
     }
}
