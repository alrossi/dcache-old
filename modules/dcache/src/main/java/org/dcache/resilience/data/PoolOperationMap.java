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
package org.dcache.resilience.data;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import org.dcache.resilience.data.PoolOperation.State;
import org.dcache.resilience.handlers.PoolOperationHandler;
import org.dcache.resilience.handlers.ResilienceMessageHandler;
import org.dcache.resilience.util.InaccessibleFileHandler;
import org.dcache.resilience.util.MapInitializer;
import org.dcache.resilience.util.Operation;
import org.dcache.resilience.util.OperationStatistics;
import org.dcache.resilience.util.PoolScanTask;
import org.dcache.resilience.util.PoolSelectionUnitDecorator.SelectionAction;
import org.dcache.util.RunnableModule;
import org.dcache.vehicles.resilience.AddPoolToPoolGroupMessage;
import org.dcache.vehicles.resilience.RemovePoolMessage;

/**
 * <p>Maintains three queues corresponding to the IDLE, WAITING, and RUNNING
 * pool operation states.  The runnable method periodically scans the
 * queues, promoting tasks as slots become available, and returning
 * pool operation placeholders to IDLE when the related scan completes.</p>
 *
 * <p>When a pool status DOWN message is received, a certain grace period
 * is observed before actually launching the associated task.</p>
 *
 * <p>Subsequent duplicate messages are handled according to a transition
 * table which defines whether the current operation should be kept, replaced
 * or cancelled (see #update).</p>
 *
 * <p>The map is swept every period which should be less than or equal to that
 *    defined by the grace interval.  Idle pools are first checked
 *    for expired "last scan" timestamps; those eligible are promoted to
 *    the waiting queue (this is the "watchdog" component of the map).  Next,
 *    the waiting queue is scanned for grace interval expiration; the
 *    eligible operations are then promoted to running, with a scan task
 *    being launched.</p>
 *
 * <p>When a scan terminates, the update of the task records whether it
 *    completed successfully, was cancelled or failed, and the task is
 *    returned to the idle queue.</p>
 *
 * <p>Map provides methods for cancellation of running pool scans, and for
 *    ad hoc submission of a scan.</p>
 *
 * <p>If pools are added or removed from the {@link PoolInfoMap} via the
 *    arrival of a PoolSelectionUnit-related messaged, the corresponding
 *    pool operation will also be added or removed here.</p>
 *
 * <p>Class is not marked final for stubbing/mocking purposes.</p>
 *
 * Created by arossi on 8/3/15.
 */
public class PoolOperationMap extends RunnableModule {
    private static final double GB = 1024*1024*1024.0;

    class Watchdog {
        Integer  rescanWindow;
        TimeUnit rescanWindowUnit;
        volatile boolean running        = true;
        volatile boolean resetInterrupt = false;
        volatile boolean runInterrupt   = false;

        long getExpiry() {
            if (!running) {
                return Long.MAX_VALUE;
            }
            return rescanWindowUnit.toMillis(rescanWindow);
        }
    }

    final Map<String, PoolOperation> idle    = new LinkedHashMap<>();
    final Map<String, PoolOperation> waiting = new LinkedHashMap<>();
    final Map<String, PoolOperation> running = new HashMap<>();

    private final Lock      lock      = new ReentrantLock();
    private final Condition condition = lock.newCondition();
    private final Watchdog  watchdog  = new Watchdog();

    private PoolInfoMap             poolInfoMap;
    private PoolOperationHandler    handler;
    private InaccessibleFileHandler inaccessibleFileHandler;

    private int                 downGracePeriod;
    private TimeUnit            downGracePeriodUnit;
    private boolean             handleRestarts;
    private int                 maxConcurrentRunning;
    private OperationStatistics counters;

    /**
     * @see #addPool(String)
     * @see ResilienceMessageHandler#messageArrived(AddPoolToPoolGroupMessage)
     */
    public void add(String pool) {
        lock.lock();
        try {
            addPool(pool);
        } finally {
            lock.unlock();
        }
    }

    /**
     * <p>Called by the admin interface.</p>
     *
     * @return the number of pool operations which have been cancelled.
     */
    public long cancel(PoolMatcher filter) {
        lock.lock();

        long cancelled = 0;
        try {
            if (filter.matchesRunning()) {
                cancelled += cancel(running, filter);
            }

            if (filter.matchesWaiting()) {
                cancelled += cancel(waiting, filter);
            }

            condition.signalAll();
        } finally {
            lock.unlock();
        }

        return cancelled;
    }

    public int getDownGracePeriod() {
        return downGracePeriod;
    }

    public TimeUnit getDownGracePeriodUnit() {
        return downGracePeriodUnit;
    }

    public PoolStatusForResilience getLastStatus(String pool) {
        return get(pool).poolStatus;
    }

    public int getMaxConcurrentRunning() {
        return maxConcurrentRunning;
    }

    public int getScanWindow() {
        return watchdog.rescanWindow;
    }

    public TimeUnit getScanWindowUnit() {
        return watchdog.rescanWindowUnit;
    }

    public String getState(String pool) {
        PoolOperation operation = get(pool);
        if (operation == null) {
            return null;
        }
        return operation.state.name();
    }

    public boolean isHandleRestarts() {
        return handleRestarts;
    }

    public boolean isWatchdogOn() {
        return watchdog.running;
    }

    /**
     * Called from admin interface.
     */
    public String list(PoolMatcher filter) {
        StringBuilder builder = new StringBuilder();
        Map<String, PoolOperation> tmp = new LinkedHashMap<>();

        lock.lock();

        try {
            if (filter.matchesRunning()) {
                tmp.putAll(running);
            }

            if (filter.matchesWaiting()) {
                tmp.putAll(waiting);
            }

            if (filter.matchesIdle()) {
                tmp.putAll(idle);
            }
        } finally {
            lock.unlock();
        }

        int total = 0;

        for (Entry<String, PoolOperation> entry : tmp.entrySet()) {
            String pool = entry.getKey();
            PoolOperation op = entry.getValue();
            if (filter.matches(pool, op)) {
                builder.append(pool).append("\t").append(op).append("\n");
                ++total;
            }
        }

        if (total == 0) {
            builder.setLength(0);
            builder.append("NO (MATCHING) OPERATIONS.\n");
        } else {
            builder.append("TOTAL OPERATIONS:\t\t").append(total).append("\n");
        }

        return builder.toString();
    }

    /**
     * @see MapInitializer#initialize()
     * @see MapInitializer#reloadAndScan()
     */
    public void loadPools() {
        lock.lock();
        try {
            running.clear();
            waiting.clear();
            idle.clear();
            poolInfoMap.getResilientPools().stream().forEach(this::addPool);
        } finally {
            lock.unlock();
        }
    }

    /**
     * @see ResilienceMessageHandler#messageArrived(RemovePoolMessage)
     * @see ResilienceMessageHandler#messageArrived(AddPoolToPoolGroupMessage)
     */
    public void remove(String pool) {
        lock.lock();
        try {
            if (running.containsKey(pool)) {
                running.remove(pool).task.cancel();
            } else if (waiting.remove(pool) == null) {
                idle.remove(pool);
            }
            condition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public void reset() {
        watchdog.resetInterrupt = true;
        threadInterrupt();
    }

    @Override
    public void run() {
        while (true) {
            lock.lock();
            try {
                condition.await(timeout, timeoutUnit);
            } catch (InterruptedException e) {
                if (watchdog.resetInterrupt) {
                    LOGGER.trace("Pool watchdog reset, returning to wait: "
                                                 + "timeout {} {}.", timeout,
                                 timeoutUnit);
                    watchdog.resetInterrupt = false;
                    continue;
                }
                if (!watchdog.runInterrupt) {
                    LOGGER.trace("Pool watchdog wait was interrupted; exiting.");
                    break;
                }
                watchdog.runInterrupt = false;
            } finally {
                lock.unlock();
            }

            LOGGER.trace("Pool watchdog initiating scan.");
            scan();
            LOGGER.trace("Pool watchdog scan completed.");
        }
    }

    public void runNow() {
        watchdog.runInterrupt = true;
        threadInterrupt();
    }

    /**
     * <p>Will <i>not</i> override the behavior
     * of normal task submission by cancelling any outstanding task for this pool.</p>
     *
     * <p>Bypasses the transition checking of last-to-next state to force
     * the task onto the waiting queue.</p>
     *
     * @true only if operation has been promoted from idle to waiting.
     */
    public boolean scan(PoolStateUpdate update) {
        lock.lock();
        try {
            if (running.containsKey(update.pool)) {
                LOGGER.warn("Scan of {} is already in progress", update.pool);
                return false;
            }

            PoolOperation operation;

            if (waiting.containsKey(update.pool)) {
                LOGGER.warn("Scan of {} is already in waiting state",
                            update.pool);
                return false;
            }

            operation = idle.remove(update.pool);
            if (operation == null) {
                LOGGER.warn("No entry for {} in any queues; "
                                            + "pool is not (yet) registered.",
                            update.pool);
                return false;
            }

            operation.forceScan = true;
            if (update.status == null ||
                            update.status == PoolStatusForResilience.UP) {
                /*
                 * Escalate status so it will pass through the wait scan.
                 */
                operation.poolStatus = PoolStatusForResilience.RESTART;
            } else {
                operation.poolStatus = update.status;
            }

            operation.lastUpdate = System.currentTimeMillis();
            operation.state = State.WAITING;
            operation.psuAction = update.action;
            operation.group = update.group;

            if (update.storageUnit != null) {
                operation.unit = poolInfoMap.getGroupIndex(update.storageUnit);
            }

            operation.exception = null;
            operation.task = null;
            waiting.put(update.pool, operation);
            return true;
        } finally {
            lock.unlock();
        }
    }

    public void setCounters(OperationStatistics counters) {
        this.counters = counters;
    }

    public void setDownGracePeriod(int downGracePeriod) {
        this.downGracePeriod = downGracePeriod;
    }

    public void setDownGracePeriodUnit(TimeUnit downGracePeriodUnit) {
        this.downGracePeriodUnit = downGracePeriodUnit;
    }

    public void setHandler(PoolOperationHandler handler) {
        this.handler = handler;
    }

    public void setInaccessibleFileHandler(InaccessibleFileHandler handler) {
        this.inaccessibleFileHandler = handler;
    }

    public void setMaxConcurrentRunning(int maxConcurrentRunning) {
        this.maxConcurrentRunning = maxConcurrentRunning;
    }

    public void setPoolInfoMap(PoolInfoMap poolInfoMap) {
        this.poolInfoMap = poolInfoMap;
    }

    public void setRescanWindow(int rescanWindow) {
        watchdog.rescanWindow = rescanWindow;
    }

    public void setRescanWindowUnit(TimeUnit rescanWindowUnit) {
        watchdog.rescanWindowUnit = rescanWindowUnit;
    }

    public void setHandleRestarts(boolean handleRestarts) {
        this.handleRestarts = handleRestarts;
    }

    public void setWatchdog(boolean on) {
        watchdog.running = on;
    }

    /**
     * <p>Called upon receipt of a pool status changed message.</p>
     */
    public void update(PoolStateUpdate update) {
        /*
         *  If the pool is not mapped, this will return false.
         */
        if (!poolInfoMap.isResilientPool(update.pool)) {
            return;
        }

        lock.lock();
        try {
            boolean active = false;

            Map<String, PoolOperation> queue = running;
            PoolOperation operation = queue.get(update.pool);

            if (operation != null) {
                active = true;
            } else {
                queue = waiting;
                operation = queue.get(update.pool);
                if (operation == null) {
                    queue = idle;
                    operation = queue.get(update.pool);
                }
            }

            PoolStatusForResilience current = operation.poolStatus;

            PoolStatusForResilience next = current == null ?
                            update.status :
                            current.getNext(update.status);

            LOGGER.trace("{}, current {}, next {}.", update.pool, current, next);

            if (next == PoolStatusForResilience.NOP
                            || next == PoolStatusForResilience.UP
                            || next == PoolStatusForResilience.UP_IGNORE
                            || next == PoolStatusForResilience.DOWN_IGNORE) {
                return;
            }

            if (active) {
                /*
                 *  NOTE:  there is no need to cancel children here.
                 *  The task is being canceled because of a change in state, so
                 *  all waiting pnfsid tasks will find the resilience requirements
                 *  altered and act accordingly.
                 */
                operation.task.cancel();
            }

            queue.remove(update.pool);

            operation.lastUpdate = System.currentTimeMillis();
            operation.poolStatus = next;

            if (next != PoolStatusForResilience.CANCEL) {
                operation.state = State.WAITING;
                operation.group = update.group;
                if (update.storageUnit != null) {
                    operation.unit = poolInfoMap.getGroupIndex(update.storageUnit);
                }
                operation.psuAction = update.action;
                operation.exception = null;
                operation.task = null;
                LOGGER.trace("Update, putting {} on WAITING queue, {}.",
                                update.pool, operation);
                waiting.put(update.pool, operation);
            } else {
                LOGGER.trace("Update, returning {} to IDLE queue as CANCELED, {}.",
                                update.pool, operation);
                operation.state = State.CANCELED;
                reset(update.pool, operation);
            }

            condition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * <p>Called by the {@link PoolOperationHandler) when scan completes.</p>
     */
    public void update(String pool, int children, long totalBytes) {
        update(pool, children, totalBytes, (CacheException) null);
    }

    /**
     * <p>Called by the {@link PnfsOperationMap) when a child operation completes.</p>
     */
    public void update(String pool, PnfsId pnfsId) {
        LOGGER.trace("Parent {}, child operation for {} has terminated.", pool,
                     pnfsId);
        lock.lock();
        try {
            PoolOperation operation = get(pool);
            operation.incrementCompleted();
            if (operation.isComplete()) {
                terminate(pool, operation);
                condition.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * <p>Called by the {@link PoolOperationHandler)
     *      when scan completes or fails.</p>
     */
    public void update(String pool, int children, long totalBytes, CacheException exception) {
        lock.lock();
        try {
            PoolOperation operation = get(pool);
            operation.exception = exception;
            operation.setChildren(children);
            operation.setTotalGBs(totalBytes/GB);
            operation.lastScan = System.currentTimeMillis();
            operation.lastUpdate = operation.lastScan;

            if (operation.poolStatus == PoolStatusForResilience.DOWN) {
                inaccessibleFileHandler.handleInaccessibleFilesIfExistOn(pool);
            }

            if (children == 0 || operation.isComplete()) {
                terminate(pool, operation);
                condition.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    private void terminate(String pool, PoolOperation operation) {
        String operationType = Operation.get(operation.poolStatus).name();

        if (operation.exception != null) {
            operation.state = State.FAILED;
            counters.incrementOperationFailed(operationType);
        } else {
            operation.state = State.IDLE;
            counters.incrementOperation(operationType);
        }

        if (running.containsKey(pool)) {
            running.remove(pool);
        } else {
            waiting.remove(pool);
        }

        reset(pool, operation);
    }

    @VisibleForTesting
    public void scan() {
        scanIdle();
        scanWaiting();
    }

    private void addPool(String pool) {
        idle.put(pool, new PoolOperation());
        counters.registerPool(pool);
    }

    private long cancel(Map<String, PoolOperation> queue, PoolMatcher filter) {
        AtomicLong cancelled = new AtomicLong(0);

        ImmutableSet.copyOf(queue.keySet()).stream().forEach((k) -> {
            PoolOperation operation = queue.get(k);
            if (filter.matches(k, operation)) {
                cancel(k, operation, queue);
                cancelled.incrementAndGet();
            }
        });

        return cancelled.get();
    }

    private void cancel(String pool, PoolOperation operation,
                        Map<String, PoolOperation> queue) {
        if (operation.task != null) {
            operation.task.cancel();
            operation.task = null;
        }

        queue.remove(pool);
        operation.state = State.CANCELED;
        reset(pool, operation);
    }

    /**
     * @return operation, or <code>null</code> if not mapped.
     */
    private PoolOperation get(String pool) {
        PoolOperation operation = running.get(pool);

        if (operation == null) {
            operation = waiting.get(pool);
        }

        if (operation == null) {
            operation = idle.get(pool);
        }

        return operation;
    }

    private void reset(String pool, PoolOperation operation) {
        operation.lastUpdate = System.currentTimeMillis();
        operation.group = null;
        operation.unit = null;
        operation.psuAction = SelectionAction.NONE;
        operation.forceScan = false;
        operation.resetChildren();
        idle.put(pool, operation);
    }

    /**
     * <p>Lock has been acquired.</p>
     *
     * <p> The scan uses the implicit temporal ordering of puts to the linked hash
     * map to find all expired pools (they will be at the head of the list
     * maintained by the map).</p>
     */
    private void scanIdle() {
        lock.lock();
        try {
            final long now = System.currentTimeMillis();
            final long expiry = watchdog.getExpiry();

            for (Iterator<Entry<String, PoolOperation>> i
                    = idle.entrySet().iterator(); i.hasNext(); ) {
                Entry<String, PoolOperation> entry = i.next();
                String pool = entry.getKey();
                PoolOperation operation = entry.getValue();
                if (now - operation.lastScan >= expiry) {
                    i.remove();
                    operation.poolStatus = PoolStatusForResilience.RESTART;
                    operation.forceScan = true;
                    operation.state = State.WAITING;
                    waiting.put(pool, operation);
                } else {
                    /**
                     * time-ordering invariant guarantees there are
                     * no more candidates at this point
                     */
                    break;
                }
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * <p>Lock has been acquired.</p>
     */
    private void scanWaiting() {
        lock.lock();
        try {
            final long now = System.currentTimeMillis();
            final long downExpiry = downGracePeriodUnit.toMillis(downGracePeriod);

            for (Iterator<Entry<String, PoolOperation>> i
                 = waiting.entrySet().iterator(); i.hasNext(); ) {
                Entry<String, PoolOperation> entry = i.next();
                String pool = entry.getKey();
                PoolOperation operation = entry.getValue();

                if (operation.poolStatus == null) {
                    /*
                     * There could be a race between the last message
                     * and a subsequent pool state change for the pool, so
                     * this is not guaranteed to be DOWN or RESTART
                     * (UP must be handled below).
                     */
                    operation.poolStatus = poolInfoMap.getPoolStatus(pool);

                    /*
                     *  Cannot contact pool.
                     */
                    if (operation.poolStatus == null) {
                        i.remove();
                        operation.state = State.IDLE;
                        LOGGER.trace("{}, handle restarts {}, force scan: {},"
                                                     + " resetting to IDLE.",
                                     operation, handleRestarts,
                                     operation.forceScan);
                        reset(pool, operation);
                    }
                }

                switch (operation.poolStatus) {
                    case DOWN:
                        if ((operation.forceScan ||
                                        now - operation.lastUpdate >= downExpiry)
                                        && running.size() < maxConcurrentRunning) {
                            i.remove();
                            LOGGER.trace("{}, lapsed time {}, running {}: "
                                                            + "submitting.",
                                            operation,
                                            now - operation.lastUpdate,
                                            running.size());
                            submit(pool, operation);
                        }
                        break;
                    case RESTART:
                        if (!handleRestarts && !operation.forceScan) {
                            i.remove();
                            operation.state = State.IDLE;
                            LOGGER.trace("{}, handle restarts {}, force scan: {},"
                                                            + " resetting to IDLE.",
                                            operation, handleRestarts,
                                            operation.forceScan);
                            reset(pool, operation);
                        } else if (running.size() < maxConcurrentRunning) {
                            LOGGER.trace("{}, handle restarts {}, force scan: {},"
                                                            + " running {}: submitting.",
                                            operation, handleRestarts,
                                            operation.forceScan, running.size());
                            i.remove();
                            submit(pool, operation);
                        }
                        break;
                    default:
                        i.remove();
                        operation.state = State.IDLE;
                        LOGGER.trace("{}, handle restarts {}, force scan: {},"
                                                            + " resetting to IDLE.",
                                            operation, handleRestarts,
                                            operation.forceScan);
                        reset(pool, operation);
                        break;
                }
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * <p>Lock has been acquired.</p>
     */
    private void submit(String pool, PoolOperation operation) {
        operation.task = new PoolScanTask(pool,
                                          operation.poolStatus.getMessageType(),
                                          operation.psuAction,
                                          operation.group,
                                          operation.unit,
                                          handler);
        operation.state = State.RUNNING;
        operation.lastUpdate = System.currentTimeMillis();
        operation.resetChildren();
        running.put(pool, operation);
        LOGGER.trace("Submitting pool scan task for {}.", pool);
        operation.task.submit();
    }
}