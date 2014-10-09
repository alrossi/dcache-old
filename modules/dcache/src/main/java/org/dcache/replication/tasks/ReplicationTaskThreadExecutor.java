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
package org.dcache.replication.tasks;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPoolGroup;
import diskCacheV111.util.CacheException;

import org.dcache.replication.api.ReplicationEndpoints;
import org.dcache.replication.api.ReplicationMessageReceiver;
import org.dcache.replication.api.ReplicationOperationRegistry;
import org.dcache.replication.api.ReplicationQueryUtilities;
import org.dcache.replication.api.ReplicationRemoveMessageFactory;
import org.dcache.replication.api.ReplicationStatistics;
import org.dcache.replication.api.ReplicationTaskExecutor;
import org.dcache.replication.data.PnfsIdMetadata;
import org.dcache.replication.data.PoolMetadata;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Encapsulates thread pooling and execution of the various tasks of the replica
 * manager service.
 *
 * @author arossi
 */
public final class ReplicationTaskThreadExecutor implements ReplicationTaskExecutor {
    private static final Logger LOGGER
        = LoggerFactory.getLogger(ReplicationTaskThreadExecutor.class);

    private final Map<String, ExecutorService> poolScanExecutors
        = Collections.synchronizedMap(new HashMap<String, ExecutorService>());

    private final Map<String, ExecutorService> verificationExecutors
        = Collections.synchronizedMap(new HashMap<String, ExecutorService>());

    private final Map<String, ExecutorService> pnfsidExecutors
        = Collections.synchronizedMap(new HashMap<String, ExecutorService>());

    private ReplicationEndpoints hub;
    private ReplicationMessageReceiver handler;
    private ReplicationOperationRegistry map;
    private ReplicationRemoveMessageFactory reductionMessageFactory;
    private ReplicationStatistics statistics;
    private ReplicationQueryUtilities utils;

    private int verificationWorkers = 3;
    private int scanWorkers = 1;
    private int replicaWorkers = 10;

    public void initialize() {
        statistics.register(TASKS, REPLICATE);
        statistics.register(TASKS, REDUCE);
        statistics.register(TASKS, DEFICIENT);
        statistics.register(TASKS, REDUNDANT);
        statistics.register(TASKS, FULL);
    }

    public void setHandler(ReplicationMessageReceiver handler) {
        this.handler = checkNotNull(handler);
    }

    public void setHub(ReplicationEndpoints hub) {
        this.hub = checkNotNull(hub);
    }

    public void setMap(ReplicationOperationRegistry map) {
        this.map = checkNotNull(map);
    }

    public void setReductionFactory(ReplicationRemoveMessageFactory messageFactory) {
        this.reductionMessageFactory = checkNotNull(messageFactory);
    }

    public void setReplicaWorkers(int replicaWorkers) {
        this.replicaWorkers = replicaWorkers;
    }

    public void setScanWorkers(int scanWorkers) {
        this.scanWorkers = scanWorkers;
    }

    public void setStatistics(ReplicationStatistics statistics) {
        this.statistics = checkNotNull(statistics);
    }

    public void setUtils(ReplicationQueryUtilities utils) {
        this.utils = checkNotNull(utils);
    }

    public void setVerificationWorkers(int verificationWorkers) {
        this.verificationWorkers = verificationWorkers;
    }

    public void shutdown() {
        for (ExecutorService service : pnfsidExecutors.values()) {
            service.shutdown();
        }

        for (ExecutorService service : poolScanExecutors.values()) {
            service.shutdown();
        }

        for (ExecutorService service : verificationExecutors.values()) {
            service.shutdown();
        }
    }

    public void submitScanForDeficientTask(PoolMetadata poolData)
                    throws CacheException, InterruptedException {
        LOGGER.debug("Submitting deficient scan task for {}.", poolData);
        ScanForInadequateCopiesTask task
            = new ScanForInadequateCopiesTask(poolData, hub, utils, handler);
        SelectionPoolGroup poolGroup = poolData.poolGroupData.poolGroup;
        if (poolGroup == null) {
            LOGGER.debug("{} does not belong to a replicating group; skipping.",
                            poolData.pool);
            return;
        }

        submitScanTask(task, poolGroup.getName(), DEFICIENT);
    }

    public void submitScanForRedundantTask(PoolMetadata poolData)
                    throws CacheException, InterruptedException {
        LOGGER.debug("Submitting redundant scan task for {}.", poolData);
        ScanForExcessiveCopiesTask task
            = new ScanForExcessiveCopiesTask(poolData, hub, utils, handler);
        SelectionPoolGroup poolGroup = poolData.poolGroupData.poolGroup;
        if (poolGroup == null) {
            LOGGER.debug("{} does not belong to a replicating group; skipping.",
                            poolData.pool);
            return;
        }

        submitScanTask(task, poolGroup.getName(), REDUNDANT);
    }

    public void submitScanFullTask(PoolMetadata poolData)
                    throws CacheException, InterruptedException {
        LOGGER.debug("Submitting full scan task for {}.", poolData);
        ScanForAllNoncompliantCopiesTask task
            = new ScanForAllNoncompliantCopiesTask(poolData, hub, utils, handler);
        /*
         * This method is called by the scanner, so it is guaranteed
         * that the pool group being scanned is replicating.
         */
        submitScanTask(task, poolData.poolGroupData.poolGroup.getName(), FULL);
    }

    public void submitReplicateTask(PnfsIdMetadata opData) {
        LOGGER.debug("Submitting replication task for {}.", opData);
        ReplicatePnfsIdTask task = new ReplicatePnfsIdTask(opData,
                                                           hub,
                                                           utils,
                                                           map,
                                                           statistics);
        submitRequestTask(task,
                          opData.poolGroupData.poolGroup.getName(),
                          REPLICATE);
    }

    public void submitReduceTask(PnfsIdMetadata opData) {
        LOGGER.debug("Submitting reduction task for {}.", opData);
        ReducePnfsIdTask task = new ReducePnfsIdTask(opData,
                                                     hub,
                                                     utils,
                                                     map,
                                                     statistics,
                                                     reductionMessageFactory,
                                                     handler);
        submitRequestTask(task,
                          opData.poolGroupData.poolGroup.getName(),
                          REDUCE);
    }

    public void submitVerifyTask(PnfsIdMetadata opData) {
        VerifyPnfsIdTask task = new VerifyPnfsIdTask(opData, hub, utils, this, map);
        Executor executor
            = getExecutorForVerification(opData.poolGroupData.poolGroup.getName());
        executor.execute(new FutureTask(task));
    }

    private ExecutorService getExecutorForPnfsidRequest(String poolGroup) {
        ExecutorService service = pnfsidExecutors.get(poolGroup);
        if (service == null) {
            service = Executors.newFixedThreadPool(replicaWorkers);
            pnfsidExecutors.put(poolGroup, service);
        }
        return service;
    }

    private ExecutorService getExecutorForPoolScan(String poolGroup) {
        ExecutorService service = poolScanExecutors.get(poolGroup);
        if (service == null) {
            service = Executors.newFixedThreadPool(scanWorkers);
            poolScanExecutors.put(poolGroup, service);
        }
        return service;
    }

    private ExecutorService getExecutorForVerification(String poolGroup) {
        ExecutorService service = verificationExecutors.get(poolGroup);
        if (service == null) {
            service = Executors.newFixedThreadPool(verificationWorkers);
            verificationExecutors.put(poolGroup, service);
        }
        return service;
    }

    private void submitRequestTask(PnfsIdRequestTask task,
                                   String poolGroup,
                                   String category) {
        Executor executor = getExecutorForPnfsidRequest(poolGroup);
        executor.execute(new FutureTask(task));
        statistics.increment(TASKS, category);
    }

    private void submitScanTask(CacheLocationsTask task,
                                String poolGroup,
                                String category) {
        Executor executor = getExecutorForPoolScan(poolGroup);
        task.setExecutor(executor);
        executor.execute(new FutureTask(task));
        statistics.increment(TASKS, category);
    }
}
