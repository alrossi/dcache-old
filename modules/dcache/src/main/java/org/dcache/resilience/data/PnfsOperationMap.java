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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import org.dcache.pool.migration.PoolMigrationCopyFinishedMessage;
import org.dcache.resilience.handlers.PnfsOperationHandler;
import org.dcache.resilience.handlers.PnfsTaskCompletionHandler;
import org.dcache.resilience.handlers.PoolTaskCompletionHandler;
import org.dcache.resilience.util.CacheExceptionUtils;
import org.dcache.resilience.util.CacheExceptionUtils.FailureType;
import org.dcache.resilience.util.CheckpointUtils;
import org.dcache.resilience.util.Operation;
import org.dcache.resilience.util.OperationHistory;
import org.dcache.resilience.util.OperationStatistics;
import org.dcache.resilience.util.PoolSelectionUnitDecorator.SelectionAction;
import org.dcache.resilience.util.ResilientFileTask;
import org.dcache.util.BackgroundForegroundDeque;
import org.dcache.util.BackgroundForegroundProcessor;
import org.dcache.util.RunnableModule;
import org.dcache.vehicles.FileAttributes;

/**
 * <p>The main locus of operations for resilience.</p>
 *
 * <p>Tracks all operations on individual files via an instance of
 * {@link PnfsOperation}, whose lifecycle is initiated by the the arrival
 * of a message, and which terminates when all work on the associated
 * pnfsid has completed or has been aborted/cancelled.</p>
 *
 * <p>When more than one event references a pnfsid which has a current
 * entry in the map, the entry's operation count is simply updated.
 * This indicates that when the current task finishes, another pass
 * should be made.  For each pass, file attributes, and hence location
 * information, is refreshed directly against the namespace. In the case
 * where there are > 1 additional passes to be made, but the current
 * state of the pnfsid locations satisfies the requirements, the
 * operation is aborted (count is set to 0, to enable removal from the
 * map).</p>
 *
 * <p>The runnable logic entails scanning the map in order to post-process
 * terminated tasks, and to launch new tasks for eligible operations if there
 * are slots available.</p>
 *
 * <p>Fairness is defined here as the availability of the first copy.
 * This means that operations are processed FIFO, but those requiring more
 * than one copy or remove task are requeued after each task has completed
 * successfully.</p>
 *
 * <p>The map distinguishes between foreground (new files) and background
 * (files on a pool being scanned) operations. The number of foreground
 * vs background operations allowed to run at each pass of the scanner
 * is balanced in proportion to the number of waiting operations on each queue.</p>
 *
 * <p>A periodic checkpointer, if on, writes out selected data from each
 * operation entry.  In the case of crash and restart of this domain,
 * the checkpoint file is reloaded into memory.</p>
 *
 * Created by arossi on 8/3/15.
 */
public final class PnfsOperationMap extends RunnableModule {
    private static final String MISSING_ENTRY =
                    "Entry for %s + was removed from map before completion of "
                                    + "outstanding operation.";

    final class Checkpointer implements Runnable {
        long     last;
        long     expiry;
        TimeUnit expiryUnit;
        String   path;
        Thread   thread;

        volatile boolean running        = false;
        volatile boolean resetInterrupt = false;
        volatile boolean runInterrupt   = false;

        public void run() {
            running = true;

            while (running) {
                try {
                    synchronized (this) {
                        wait(expiryUnit.toMillis(expiry));
                    }
                } catch (InterruptedException e) {
                    if (resetInterrupt) {
                        LOGGER.trace("Checkpoint reset: expiry {} {}.",
                                     expiry, expiryUnit);
                        resetInterrupt = false;
                        continue;
                    }

                    if (!runInterrupt) {
                        LOGGER.trace("Consumer wait was interrupted; exiting.");
                        break;
                    }
                    runInterrupt = false;
                }

                save();
            }
        }

        /**
         * Writes out data from the operation map to the checkpoint file.
         */
        @VisibleForTesting
        void save() {
            long start = System.currentTimeMillis();
            long count = CheckpointUtils.save(path, poolInfoMap, deque);
            last = System.currentTimeMillis();
            counters.recordCheckpoint(last, last - start, count);
        }
    }

    abstract class PnfsOperationProcessor
                    implements BackgroundForegroundProcessor<PnfsOperation> {
        protected final List<PnfsOperation> toProcess = new ArrayList<>();

        @Override
        public void processForeground(Iterator<PnfsOperation> iterator) {
            process(iterator, Type.FOREGROUND);
        }

        @Override
        public void processBackground(Iterator<PnfsOperation> iterator) {
            process(iterator, Type.BACKGROUND);
        }

        protected abstract void process(Iterator<PnfsOperation> iterator, Type type);

        protected void reset() {
            toProcess.clear();
        }
    }

    final class TerminalOperationProcessor extends PnfsOperationProcessor {
        @Override
        protected void process(Iterator<PnfsOperation> iterator, Type type) {
            while (iterator.hasNext()) {
                PnfsOperation operation = iterator.next();
                switch (operation.getState()) {
                    case PnfsOperation.RUNNING:
                        continue;
                    case PnfsOperation.VOID:
                    case PnfsOperation.ABORTED:
                    case PnfsOperation.CANCELED:
                    case PnfsOperation.DONE:
                    case PnfsOperation.FAILED:
                        toProcess.add(operation);
                        LOGGER.trace("{} operation for {} terminated.",
                                     type, operation.getPnfsId());
                        continue;
                    case PnfsOperation.WAITING:
                    default:
                        /*
                         * queue ordering guarantees there are only
                         * WAITING tasks from this point on.
                         */
                        return;
                }
            }
        }
    }

    final class WaitingOperationProcessor extends PnfsOperationProcessor {
        long foreground;
        long background;

        @Override
        protected void process(Iterator<PnfsOperation> iterator, Type type) {
            long toPromote = 0;
            long limit = getLimit(type);

            while (iterator.hasNext()) {
                PnfsOperation operation = iterator.next();
                switch (operation.getState()) {
                    case PnfsOperation.WAITING:
                        if (++toPromote > limit) {
                            return;
                        }

                        LOGGER.trace("adding {} op {} for {}", type,
                                     toPromote, operation.getPnfsId());
                        toProcess.add(operation);
                        break;
                    default:
                }
            }
        }

        protected void reset() {
            super.reset();
            foreground = 0;
            background = 0;
        }

        /**
         * <p>The number of waiting operations which can be promoted
         *    to running is based on the available slots (maximum minus
         *    the number of operations which have completed since the last
         *    pass).</p>
         *
         * <p>The proportion allotted to foreground vs background is based
         *    on the proportion of operations in the queues.</p>
         *
         * <p>If the proportion rounds to 0, but the queue is not empty,
         *    one of the waiting tasks will be run.</p>
         */
        void computeAvailable(long current) {
            long available = copyThreads - current;

            if (available == 0) {
                foreground = 0;
                background = 0;
                return;
            }

            /*
             *  These two values are not acquired atomically, but for the purpose
             *  of determining availability, the approximation is good enough.
             */
            double fgsize = deque.foregroundSize();
            double bgsize = deque.backgroundSize();
            double size = fgsize+bgsize;

            if (size == 0.0) {
                foreground = 0;
                background = 0;
                return;
            }

            double fgweight = fgsize / size;

            foreground = Math.round(available * fgweight);
            background = available - foreground;

            if (foreground == 0 && fgsize > 0) {
                foreground = 1;
                --background;
            } else if (background == 0 && bgsize > 0) {
                background = 1;
                --foreground;
            }

            LOGGER.trace("running {}, available {}, foreground {}, background {}",
                         current, available, foreground, background);
        }

        private long getLimit(Type type) {
            switch (type) {
                case FOREGROUND: return foreground;
                case BACKGROUND: return background;
                default:
                    return 0L;
            }
        }
    }

    /**
     * The order for election to run is FIFO.
     * An attempt at fairness is maintained by removing and reinserting
     * the entry when it successfully terminates, if more work is to be done.
     * As operations are promoted to running, they are moved to the front
     * of their queue.
     */
    final BackgroundForegroundDeque<PnfsId, PnfsOperation> deque
                    = new BackgroundForegroundDeque<>();

    final TerminalOperationProcessor terminal = new TerminalOperationProcessor();
    final WaitingOperationProcessor  waiting  = new WaitingOperationProcessor();
    final AtomicLong                 running  = new AtomicLong(0);

    /*
     * For recovery.
     */
    @VisibleForTesting
    final Checkpointer checkpointer = new Checkpointer();

    private PoolInfoMap               poolInfoMap;
    private PnfsOperationHandler      operationHandler;
    private PnfsTaskCompletionHandler completionHandler;
    private PoolTaskCompletionHandler poolTaskCompletionHandler;

    /**
     * This number serves as the upper limit on the number of tasks
     * which can be run at a given time.  This ensures the tasks
     * will not block on the executor queue waiting for a thread.
     * To ensure the task does not block in general, the number of database
     * connections must be at least equal to this, but should probably be 1 1/2
     * to 2 times greater to allow other operations on Chimera to run as well.
     * See the default settings.
     *
     * (Note:  remove calls are currently synchronous (to guarantee proper
     * pinning/unpinning, but they complete quickly); so when removes are
     * in progress, the thread state will show it is blocked; but this is not
     * from resource contention.)
     */
    private int copyThreads = 200;
    private int maxRetries  = 2;

    /**
     * Statistics collection.
     */
    private OperationStatistics counters;
    private OperationHistory    history;

    /**
     * @param remove true if the entire entry is to be removed from the
     *               map at the next scan.  Otherwise, cancellation pertains
     *               only to the current (running) operation.
     */
    public void cancel(PnfsId pnfsId, boolean remove) {
        PnfsOperation operation = deque.get(pnfsId);

        if (operation == null) {
            throw new IllegalStateException(
                            String.format(MISSING_ENTRY, pnfsId));
        }

        operation.cancelCurrent();
        if (remove) {
            operation.setOpCount((short) 0); // force removal
        }

        if (operation.getState() == PnfsOperation.RUNNING) {
            running.decrementAndGet();
        }

        signalAll();
    }

    /**
     * Batch version of cancel.  In this case, the filter will
     * indicate whether the operation should be cancelled in toto
     * or only the current task.
     */
    public long cancel(final PnfsMatcher filter) {
        Iterator<PnfsOperation> iterator = deque.getValueIterator();
        long cancelled = 0;
        boolean forceRemoval = filter.isForceRemoval();
        while (iterator.hasNext()) {
            PnfsOperation operation = iterator.next();
            if (filter.matches(operation, poolInfoMap)) {
                operation.cancelCurrent();
                if (forceRemoval) {
                    operation.setOpCount((short) 0); // force removal
                }
                if (operation.getState() == PnfsOperation.RUNNING) {
                    running.decrementAndGet();
                }
                ++cancelled;
            }
        }

        signalAll();
        return cancelled;
    }

    /**
     * @return number of operations matching the filter.
     */
    public long count(PnfsMatcher filter) {
        Iterator<PnfsOperation> iterator = deque.getValueIterator();
        long total = 0;
        while (iterator.hasNext()) {
            PnfsOperation operation = iterator.next();
            if (filter.matches(operation, poolInfoMap)) {
                ++total;
            }
        }
        return total;
    }

    public long getCheckpointExpiry() {
        return checkpointer.expiry;
    }

    public TimeUnit getCheckpointExpiryUnit() {
        return checkpointer.expiryUnit;
    }

    public String getCheckpointFilePath() {
        return checkpointer.path;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public int getMaxRunning() {
        return copyThreads;
    }

    public PnfsOperation getOperation(PnfsId pnfsId) {
        return deque.get(pnfsId);
    }

    public void initialize() {
        super.initialize();
        startCheckpointer();
    }

    public boolean isCheckpointingOn() {
        return checkpointer.running;
    }

    /**
     * Used by the admin command.
     */
    public String list(final PnfsMatcher filter, final int limit) {
        Iterator<PnfsOperation> iterator = deque.getValueIterator();

        StringBuilder builder = new StringBuilder();
        int total = 0;

        while (iterator.hasNext()) {
            PnfsOperation operation = iterator.next();
            if (filter.matches(operation, poolInfoMap)) {
                ++total;
                builder.append(operation).append("\n");
            }

            if (total >= limit) {
                break;
            }
        }

        if (total == 0) {
            builder.append("NO (MATCHING) OPERATIONS.\n");
        } else {
            builder.append("TOTAL OPERATIONS:\t\t").append(total).append("\n");
        }

        return builder.toString();
    }

    /**
     * <p>Called by the {@link PnfsOperationHandler}.
     * Adds essential information to a new entry.</p>
     *
     * @return true if insert returns true.
     */
    public boolean register(PnfsUpdate data) {
        PnfsOperation operation = new PnfsOperation(data.pnfsId,
                                                    data.getGroup(),
                                                    data.getUnitIndex(),
                                                    data.getSelectionAction(),
                                                    data.getCount(),
                                                    data.getSize());
        operation.setParentOrSource(data.getPoolIndex(), data.isParent);
        operation.setVerifySticky(data.shouldVerifySticky());
        FileAttributes attributes = data.getAttributes();
        operation.setLocations((short) attributes.getLocations().size());
        operation.setRetentionPolicy(
                        attributes.getRetentionPolicy().toString());
        operation.resetOperation();

        boolean inserted = deque.insert(data.pnfsId, operation);
        signalAll();

        return inserted;
    }

    /**
     * <p>Reads in the checkpoint file.  Creates one if it does not exist.
     * For each entry read, creates a {@link PnfsUpdate} and calls
     * {@link PnfsOperationHandler#handleLocationUpdate(PnfsUpdate)}.</p>
     */
    public void reload() {
        CheckpointUtils.load(checkpointer.path, poolInfoMap, operationHandler);
    }

    /**
     * <p>Called after a change to the checkpoint path and/or interval.
     *    Interrupts the thread so that it resumes with the new settings.</p>
     */
    public void reset() {
        if (isCheckpointingOn()) {
            checkpointer.resetInterrupt = true;
            checkpointer.thread.interrupt();
        }
    }

    /**
     * <p>The consumer thread. When notified or times out, iterates over
     * the internal list to check the state of running tasks and/or
     * to submit waiting tasks if there are open slots. Removes
     * completed operations.</p>
     */
    public void run() {
        try {
            while (true) {
                LOGGER.trace("Calling scan.");
                scan();
                LOGGER.trace("Scan complete, waiting ...");
                await();
            }
        } catch (IOException e) {
            LOGGER.error("Consumer thread failed, "
                                         + "resilience is no longer running.",
                         e);
        } catch (InterruptedException e) {
            LOGGER.trace("Consumer was interrupted.");
        }
    }

    /**
     * <p>If the checkpointing thread is running, interrupts the wait so that it
     *    calls save immediately.  If it is off, it just calls
     *    save (Note:  the latter is done on the caller thread;
     *    this is used mainly for testing.  For the admin command,
     *    this method is disallowed if checkpointing is off.)</p>
     */
    public void runCheckpointNow() {
        if (isCheckpointingOn()) {
            checkpointer.runInterrupt = true;
            checkpointer.thread.interrupt();
        } else {
            checkpointer.save();
        }
    }

    @VisibleForTesting
    public void scan() throws IOException {
        checkRunning();
        postProcess();
        submitReady();
    }

    public void setCheckpointExpiry(long checkpointExpiry) {
        checkpointer.expiry = checkpointExpiry;
    }

    public void setCheckpointExpiryUnit(TimeUnit checkpointExpiryUnit) {
        checkpointer.expiryUnit = checkpointExpiryUnit;
    }

    public void setCheckpointFilePath(String checkpointFilePath) {
        checkpointer.path = checkpointFilePath;
    }

    public void setCompletionHandler(
                    PnfsTaskCompletionHandler completionHandler) {
        this.completionHandler = completionHandler;
    }

    public void setCopyThreads(int copyThreads) {
        this.copyThreads = copyThreads;
    }

    public void setCounters(OperationStatistics counters) {
        this.counters = counters;
    }

    public void setHistory(OperationHistory history) {
        this.history = history;
    }

    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    public void setOperationHandler(PnfsOperationHandler operationHandler) {
        this.operationHandler = operationHandler;
    }

    public void setPoolInfoMap(PoolInfoMap poolInfoMap) {
        this.poolInfoMap = poolInfoMap;
    }

    public void setPoolTaskCompletionHandler(
                    PoolTaskCompletionHandler poolTaskCompletionHandler) {
        this.poolTaskCompletionHandler = poolTaskCompletionHandler;
    }

    public void startCheckpointer() {
        checkpointer.thread = new Thread(checkpointer, "Checkpointing");
        checkpointer.thread.start();
    }

    public void stopCheckpointer() {
        checkpointer.running = false;
        checkpointer.thread.interrupt();
    }

    /**
     * <p>Another pool/source has requested a resilience check on this file.</p>
     *
     * @return true if the pnfsId is already registered, false if it is new entry.
     */
    public boolean updateCount(PnfsId pnfsId) {
        PnfsOperation operation = deque.get(pnfsId);

        if (operation == null) {
            return false;
        }

        operation.incrementCount();

        signalAll();
        return true;
    }

    /**
     * <p>Records the selected source and/or target.
     * No state change is involved.</p>
     */
    public void updateOperation(PnfsId pnfsId, String source, String target) {
        PnfsOperation operation = deque.get(pnfsId);

        if (operation == null) {
            throw new IllegalStateException(
                            String.format(MISSING_ENTRY, pnfsId));
        }

        if (source != null) {
            operation.setSource(poolInfoMap.getPoolIndex(source));
        }

        if (target != null) {
            operation.setTarget(poolInfoMap.getPoolIndex(target));
        }
    }

    /**
     * <p>Terminal update.</p>
     */
    public void updateOperation(PnfsId pnfsId, CacheException error) {
        PnfsOperation operation = deque.get(pnfsId);

        if (operation == null) {
            throw new IllegalStateException(
                            String.format(MISSING_ENTRY, pnfsId));
        }

        operation.updateOperation(error);

        running.decrementAndGet();

        signalAll();
    }

    /**
     * <p>Migration task termination.</p>
     */
    public void updateOperation(PoolMigrationCopyFinishedMessage message) {
        PnfsId pnfsId = message.getPnfsId();

        PnfsOperation operation = deque.get(pnfsId);

        if (operation == null) {
            throw new IllegalStateException(
                            String.format(MISSING_ENTRY, pnfsId));
        }

        operation.relay(message);
    }

    /**
     * <p>Terminal update.</p>
     */
    public void voidOperation(PnfsId pnfsId) {
        PnfsOperation operation = deque.get(pnfsId);

        if (operation == null) {
            return;
        }

        operation.voidOperation();

        running.decrementAndGet();

        signalAll();
    }

    private synchronized void await() throws InterruptedException {
        wait(timeoutUnit.toMillis(timeout));
    }

    /**
     * <p>WAITING operations are launched in order of arrival,
     * and terminal are removed and (if there is more
     * work to be done) moved to the end of their queue.</p>
     *
     * <p>Hence the invariant "RUNNING | DONE | FAILED | CANCELED
     * always precede WAITING".</p>
     *
     * <p>Two separate queues are maintained internal to the deque
     * object, one for foreground (i.e., non-scan initiated) operations,
     * and one for background.</p>
     *
     * <p>The number of foreground to background operations allowed
     * to run is determined by the proportion of such operations
     * that are currently waiting.</p>
     */
    private void checkRunning() {
        /*
         *  Get the running count first.
         *  Since promotion to running is done on the thread which calls
         *  this method, running can only decrease during this
         *  computation.  Hence the available number may not always
         *  be the maximum allowed (number of copy threads), but should
         *  never exceed it.
         */
        long current = running.get();
        deque.processForeground(terminal);
        deque.processBackground(terminal);
        waiting.computeAvailable(current);
        deque.processForeground(waiting);
        deque.processBackground(waiting);
    }

    /**
     * <p>Only operations whose counts are > 0 will
     * remain in the map after this call.  If the non-zero count
     * is subsequent to a failed operation, the operation
     * maintains its position in the queue; else it is removed
     * and requeued.</p>
     */
    private void postProcess() {
        terminal.toProcess.stream().forEach((operation) -> {
            /*
             *  This call will determine if there is more
             *  work to be done. If not, the operation
             *  record will be removed from persistent storage.
             *  No more work is signified by opCount == 0.
             */
            postProcess(operation);

            byte state = operation.getState();

            boolean failed = state == PnfsOperation.FAILED
                            || state == PnfsOperation.ABORTED;
            boolean restore = operation.getOpCount() > 0;
            boolean remove = !failed || !restore;

            LOGGER.trace("post process, remove {}, restore {}, {}", remove,
                         restore, operation);

            if (remove) {
                if (restore) {
                    deque.moveToBack(operation);
                } else {
                    deque.remove(operation.getPnfsId());
                    if (operation.isBackground()) {
                        String parent = poolInfoMap.getPool(
                                        operation.getParent());
                        poolTaskCompletionHandler.childTerminated(parent,
                                                                  operation.getPnfsId());
                    }
                    history.add(operation.toHistoryString(), failed);
                }
            }
        });

        terminal.reset();
    }

    /**
     * <p>
     * Exceptions are analyzed to determine if any more work can be done.
     * In the case of fatal errors, an alarm is sent.  Operations with
     * counts > 0 are reset to waiting; otherwise, the operation record
     * will be removed when this method returns.
     * </p>
     */
    private void postProcess(PnfsOperation operation) {
        operation.setLastType();
        String pool = operation.getPrincipalPool(poolInfoMap);
        if (operation.getState() == PnfsOperation.FAILED) {
            FailureType type = CacheExceptionUtils.getFailureType(
                            operation.getException());

            switch (type) {
                case BROKEN:
                    if (operation.getSource() != null) {
                        pool = poolInfoMap.getPool(operation.getSource());
                        operationHandler.handleBrokenFileLocation(operation.getPnfsId(),
                                        pool);
                    }
                    // fall through - possibly retriable with another source
                case NEWSOURCE:
                    operation.addSourceToTriedLocations();
                    operation.resetSourceAndTarget();
                    break;
                case NEWTARGET:
                    operation.addTargetToTriedLocations();
                    operation.resetSourceAndTarget();
                    break;
                case RETRIABLE:
                    operation.incrementRetried();
                    if (operation.getRetried() < maxRetries) {
                        break;
                    }
                    operation.addTargetToTriedLocations();
                    if (operation.getLocations()
                                    > operation.getTried().size()) {
                        operation.resetSourceAndTarget();
                        break;
                    }
                    // fall through; no more possibilities left
                case FATAL:
                    operation.addTargetToTriedLocations();
                    Set<String> tried = operation.getTried().stream()
                                    .map((i) -> poolInfoMap.getPool(i))
                                    .collect(Collectors.toSet());
                    completionHandler.taskAborted(operation.getPnfsId(), tried,
                                    operation.getRetried(), maxRetries,
                                    operation.getException());
                    operation.abortOperation();
                    break;
                default:
                    String error = String.format("%s: No such failure type: %s.",
                                                 operation.getPnfsId(), type);
                    throw new IllegalStateException(error);
            }
            counters.incrementOperationFailed(Operation.PNFSID.name());
            counters.incrementFailed(pool, operation.getType());
        } else if (operation.getState() == PnfsOperation.DONE) {
            counters.incrementOperation(Operation.PNFSID.name());
            String source = null;
            String target = null;

            Integer index = operation.getSource();
            if (index != null) {
                source = poolInfoMap.getPool(index);
            }

            index = operation.getTarget();
            if (index != null) {
                target = poolInfoMap.getPool(index);
            }

            counters.increment(source, target, operation.getType(),
                               operation.getSize());
            operation.setSource(null);
            operation.setTarget(null);
        }

        if (operation.getOpCount() > 0) {
            operation.resetOperation();
        }
    }

    private synchronized void signalAll() {
        notifyAll();
    }

    /**
     * <p>Task is submitted to an appropriate executor service.</p>
     */
    private void submit(PnfsOperation operation) {
        byte action = (byte) SelectionAction.REMOVE.ordinal();
        operation.setTask(new ResilientFileTask(operation.getPnfsId(),
                        operation.getSelectionAction() == action,
                        operationHandler));
        operation.setState(PnfsOperation.RUNNING);
        running.incrementAndGet();
        deque.moveToFront(operation);
        operation.submit();
    }

    private void submitReady() {
        waiting.toProcess.stream().forEach((operation) -> submit(operation));
        waiting.reset();
    }
}
