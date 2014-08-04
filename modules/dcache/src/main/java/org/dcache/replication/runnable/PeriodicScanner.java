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
package org.dcache.replication.runnable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPoolGroup;
import diskCacheV111.util.CacheException;

import org.dcache.replication.api.ReplicationEndpoints;
import org.dcache.replication.api.ReplicationQueryUtilities;
import org.dcache.replication.api.ReplicationTaskExecutor;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Periodically looks at the pool groups whose constraints suggest
 * the need for file replication, then takes the active pools in the
 * group and submits a scan task for each which will reveal which
 * PnfsIds are in need of some corrective action (either replicate or
 * reduce).<br><br>
 *
 * Note that because of the partitioning requirement that a given pool
 * not belong to more than one replicating group, each pool
 * should be processed at most once per pass.
 *
 * @author arossi
 */
public final class PeriodicScanner extends RunnableModule {
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong nextCheck = new AtomicLong(System.currentTimeMillis());

    private ReplicationEndpoints hub;
    private ReplicationTaskExecutor executor;
    private ReplicationQueryUtilities utils;

    public long getNextScan() {
        return nextCheck.get();
    }

    public void initialize() {
        checkNotNull(hub);
        checkNotNull(executor);
        checkNotNull(utils);
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
                doScan();
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

    public void setHub(ReplicationEndpoints hub) {
        this.hub = hub;
    }

    public void setExecutor(ReplicationTaskExecutor executor) {
        this.executor = executor;
    }

    public void setUtils(ReplicationQueryUtilities utils) {
        this.utils = utils;
    }

    public void shutdown() {
        running.set(false);
        super.shutdown();
    }

    private void doScan() {
        PoolSelectionUnit psu = null;

        try {
            psu = hub.getPoolMonitor().getPoolSelectionUnit();
        } catch (CacheException ce) {
            LOGGER.error("A problem occurred while trying to "
                            + "fetch selection unit: {}.",
                            ce.getMessage());
        } catch (InterruptedException ie ) {
            LOGGER.warn("Thread interrupted while trying to, "
                            + "fetch selection unit; "
                            + "cannot complete scan "
                            + "at this time.");
        }

        if (psu != null) {
            List<SelectionPoolGroup> resilientGroups = null;

            try {
                resilientGroups = findAllResilientGroups(psu);
            } catch (CacheException ce) {
                LOGGER.error("A problem occurred while processing "
                                + "pool groups: {}.",
                                ce.getMessage());
                LOGGER.debug("run(): pool selection unit problem", ce);
            } catch (InterruptedException ie) {
                LOGGER.warn("Thread interrupted during pool monitor check, "
                                + "cannot complete scan at this time.");
            }

            if (resilientGroups != null) {
                for (SelectionPoolGroup group : resilientGroups) {
                    utils.scanActivePools(group, psu);
                }
            }
        }
    }

    private List<SelectionPoolGroup> findAllResilientGroups(PoolSelectionUnit psu)
                    throws CacheException, InterruptedException {
        Map<String,SelectionPoolGroup> all = psu.getPoolGroups();
        List<SelectionPoolGroup> resilient = new ArrayList<>();
        for (SelectionPoolGroup group: all.values()) {
            if (group.getMinReplicas() > 1) {
                resilient.add(group);
            }
        }
        return resilient;
    }
}
