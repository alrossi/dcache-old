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
package org.dcache.namespace.replication.monitoring;

import com.google.common.base.Splitter;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PnfsClearCacheLocationMessage;
import diskCacheV111.vehicles.PoolStatusChangedMessage;
import org.dcache.namespace.replication.workers.AbstractUpdateWorker;
import org.dcache.namespace.replication.workers.PnfsUpdateWorker;
import org.dcache.namespace.replication.workers.PoolScanWorker;
import org.dcache.namespace.replication.workers.PoolStatusUpdateWorker;
import org.dcache.vehicles.PnfsSetFileAttributes;

/**
 * This module provides a record of the operations which are in flight,
 * by worker category. Once a worker completes or fails, its record
 * is removed from the list.
 * <p/>
 * A counter is used to keep track of the cumulative totals of both
 * operations and messages received.
 * <p/>
 * Queries for listings can be run over date ranges,
 * pool name, pnfsid, and operation type.
 * <p/>
 * In addition to the monitoring functions, the registry
 * maintains a map of workers, allowing for cancellation
 * of running jobs.
 *
 * Created by arossi on 1/30/15.
 */
public final class ActivityRegistry {
    public static final DateFormat DATE_FORMAT
                    = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    private static final String MESSAGE = "MESSAGE";
    private static final String OPERATION = "OPERATION";
    private static final String CLEAR_CACHE_LOCATION = "CLEAR_CACHE_LOCATION";
    private static final String NEW_FILE_LOCATION = "NEW_FILE_LOCATION";
    private static final String POOL_STATUS_DOWN = "POOL_STATUS_DOWN";
    private static final String POOL_STATUS_RESTART = "POOL_STATUS_RESTART";

    enum Operation {
        PNFSID,
        POOL_DOWN,
        POOL_RESTART,
        POOL_SCAN;

        static boolean validate(String expression) {
            String lowerCase = expression.toLowerCase();
            return lowerCase.contains("down") ||
                            lowerCase.contains("restart") ||
                            lowerCase.contains("pnfsid")||
                            lowerCase.contains("scan");
        }

        /*
         * Call only after validate.
         */
        static Operation getOperation(String expression) {
            String lowerCase = expression.toLowerCase();
            if (lowerCase.contains("down")) {
                return Operation.POOL_DOWN;
            } else if (lowerCase.contains("restart")) {
                return Operation.POOL_RESTART;
            } else if (lowerCase.contains("pnfsid")) {
                return Operation.PNFSID;
            } else {
                return Operation.POOL_SCAN;
            }
        }
    }

    class Filter {
        final Date before;
        final Date after;
        final Set<String> pools;
        final Set<String> pnfsids;
        final Set<Operation> operations;

        Filter(Date before,
               Date after,
               Set<String> pools,
               Set<String> pnfsids,
               Set<Operation> operations) {
            this.before = before;
            this.after = after;
            this.pools = pools;
            this.pnfsids = pnfsids;
            this.operations = operations;
        }

        Filter(String before,
                      String after,
                      String poolList,
                      String pnfsidList,
                      String operationList) throws ParseException {
            if (before != null) {
                this.before = DATE_FORMAT.parse(before);
            } else {
                this.before = null;
            }

            if (after != null) {
                this.after = DATE_FORMAT.parse(after);
            } else {
                this.after = null;
            }

            if (poolList != null) {
                this.pools = new HashSet<>(Splitter.on(",")
                                                   .splitToList(poolList));
            } else {
                this.pools = null;
            }

            if (pnfsidList != null) {
                this.pnfsids = new HashSet<>(Splitter.on(",")
                                                   .splitToList(pnfsidList));
            } else {
                this.pnfsids = null;
            }

            if (operationList != null) {
                this.operations
                    = Splitter.on(",").splitToList(pnfsidList)
                              .stream()
                              .filter((s) -> Operation.validate(s))
                              .map((s) -> Operation.getOperation(s))
                              .collect(Collectors.toSet());
            } else {
                this.operations = null;
            }
        }
    }

    class ActivityRecord {
        final UUID id;
        final Date started;
        final String pool;
        final String pnfsid;
        final Operation op;

        ActivityRecord(UUID id, String pool, Operation op) {
            this(id, pool, null, op);
        }

        ActivityRecord(UUID id, String pool, PnfsId pnfsId, Operation op) {
            this.id = id;
            this.pool = pool;
            this.op = op;
            this.pnfsid = pnfsId.toIdString();
            started = new Date(System.currentTimeMillis());
        }

        boolean matches(Filter filter) {
            if (filter.before != null && !started.before(filter.before)) {
                return false;
            }
            if (filter.after != null && !started.after(filter.after)) {
                return false;
            }
            if (filter.pools != null && !filter.pools.contains(pool)) {
                return false;
            }
            if (filter.pnfsids != null && (pnfsid == null ||
                            !filter.pnfsids.contains(pnfsid))) {
                return false;
            }
            if (filter.operations != null && !filter.operations.contains(op)) {
                return false;
            }
            return true;
        }

        @Override
        public String toString() {
            return started + "\t" + op + "\t" + pool
                            + (pnfsid == null ? "" : pnfsid);
        }
    }

    private final LinkedHashMap<UUID, ActivityRecord> registry
                    = new LinkedHashMap<>();
    private final OperationCounters counters = new OperationCounters();
    private final Map<String, AbstractUpdateWorker> running =
                    Collections.synchronizedMap(new HashMap<>());

    public ActivityRegistry() {
        counters.register(MESSAGE, NEW_FILE_LOCATION);
        counters.register(MESSAGE, CLEAR_CACHE_LOCATION);
        counters.register(MESSAGE, POOL_STATUS_DOWN);
        counters.register(MESSAGE, POOL_STATUS_RESTART);
        counters.register(OPERATION, Operation.PNFSID.toString());
        counters.register(OPERATION, Operation.POOL_DOWN.toString());
        counters.register(OPERATION, Operation.POOL_RESTART.toString());
        counters.register(OPERATION, Operation.POOL_SCAN.toString());
    }

    public void cancel(String name) throws NoSuchElementException {
        AbstractUpdateWorker worker = running.remove(name);
        if (worker != null) {
            worker.getWorkerFuture().cancel(true);
            registry.remove(worker.workerId);
        } else {
            throw new NoSuchElementException("Could not find worker for " + name);
        }
    }

    public String list(String before,
                       String after,
                       String pools,
                       String pnfsids,
                       String operations,
                       int limit) throws ParseException {
        Filter filter = new Filter(before, after, pools, pnfsids, operations);
        return list(filter, limit);
    }

    public String list(Date before,
                       Date after,
                       Set<String> pool,
                       Set<String> pnfsids,
                       Set<Operation> operations,
                       int limit) {
        Filter filter = new Filter(before, after, pool, pnfsids, operations);
        return list(filter, limit);
    }

    public synchronized String printTotals() {
        return counters.print();
    }

    public synchronized void register(PoolStatusChangedMessage message) {
        switch(message.getPoolState()) {
            case PoolStatusChangedMessage.DOWN:
                counters.increment(MESSAGE, POOL_STATUS_DOWN);
                break;
            case PoolStatusChangedMessage.RESTART:
                counters.increment(MESSAGE, POOL_STATUS_RESTART);
                break;
        }
    }

    public synchronized void register(PnfsClearCacheLocationMessage message) {
        counters.increment(MESSAGE, CLEAR_CACHE_LOCATION);
    }

    public synchronized void register(PnfsSetFileAttributes message) {
        counters.increment(MESSAGE, NEW_FILE_LOCATION);
    }

    public synchronized void register(PnfsUpdateWorker worker) {
        ActivityRecord record = new ActivityRecord(worker.workerId,
                                                   worker.getPoolName(),
                                                   worker.getPnfsId(),
                                                   Operation.PNFSID);
        add(record);
        running.put(worker.getWorkerName(), worker);
    }

    public synchronized void register(PoolScanWorker worker) {
        ActivityRecord record = new ActivityRecord(worker.workerId,
                                                   worker.getPoolName(),
                                                   Operation.POOL_SCAN);
        add(record);
        running.put(worker.getWorkerName(), worker);
    }

    public synchronized void register(PoolStatusUpdateWorker worker) {
        Operation op = null;
        switch (worker.getType()) {
            case DOWN:      op = Operation.POOL_DOWN;    break;
            case RESTART:   op = Operation.POOL_RESTART; break;
        }

        ActivityRecord record = new ActivityRecord(worker.workerId,
                                                   worker.getPoolName(),
                                                   op);
        add(record);
        running.put(worker.getWorkerName(), worker);
    }

    public synchronized void unregister(AbstractUpdateWorker worker) {
        registry.remove(worker.workerId);
        running.remove(worker.getWorkerName());
    }

    private void add(ActivityRecord record) {
        registry.put(record.id, record);
        counters.increment(OPERATION, record.op.toString());
    }

    public synchronized void failed(AbstractUpdateWorker worker) {
        ActivityRecord record = registry.remove(worker.workerId);
        running.remove(worker.getWorkerName());
        counters.incrementFailed(OPERATION, record.op.toString());
    }

    private synchronized String list(Filter filter, int limit) {
        StringBuilder builder = new StringBuilder();
        int count = 0;
        for (ActivityRecord record: registry.values()) {
            if (count > limit) {
                break;
            }

            if (record.matches(filter)) {
                builder.append(record).append("\n");
                count++;
            }
        }
        builder.insert(0, "TOTAL:\t\t" + count + "\n\n");
        return builder.toString();
    }
}
