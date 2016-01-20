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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Set;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import org.dcache.pool.migration.PoolMigrationCopyFinishedMessage;
import org.dcache.resilience.handlers.PnfsOperationHandler;
import org.dcache.resilience.handlers.PnfsOperationHandler.Type;
import org.dcache.resilience.util.ResilientFileTask;
import org.dcache.util.Backgroundable;
import org.dcache.util.CellStubFactory;
import org.dcache.util.ExceptionMessage;
import org.dcache.vehicles.resilience.EnsureSystemStickyBitMessage;

/**
 * <p>Object stored in the {@link PnfsOperationMap}.</p>
 *
 * <p>Since this table may grow very large, two strategies have been
 * adopted to try to reduce the memory footprint of each instance:</p>
 *
 * <ol>
 * <li>Enums are replaced by static byte values and conversion methods.</li>
 * <li>Only Integer indices referencing the {@link PoolInfoMap} are stored.</li>
 * </ol>
 *
 * <p>The latter choice of course will have some impact on performance, but
 * the trade-off is necessitated by the decision to use only a simple
 * persistence model for rollback/recovery purposes and
 * otherwise rely on values stored in memory.</p>
 *
 * <p>A note on synchronization.  There are only three points of real
 *     contention possible on an operation object.</p>
 *
 * <ol>
 *  <li>Between various threads on the entry point operations
 *      {@link PnfsOperationHandler#handleLocationUpdate(PnfsUpdate)}
 *      {@link PnfsOperationHandler#handleScannedLocation(PnfsUpdate, Integer)}</li>
 *  <li>The task thread or arriving completion message thread setting state and the consumer reading state.</li>
 *  <li>The admin thread and the consumer setting state/operation on cancel.</li>
 * </ol>
 *
 * <p>In the first case, we need only synchronize on the opCount;
 *      in the second case, on state.  The third case again involves these
 *      two attributes, so they again must by synchronized.
 *      In all other cases, modification of the object attributes belongs
 *      solely to either the task or the consumer at distinct times, ie.
 *      at implicitly serializable instances.</p>
 *
 * <p>For the purposes of efficiency, we allow purely read-only access
 *    (such as through the admin interface or the checkpointing operation)
 *    to be unsynchronized.</p>
 *
 * Created by arossi on 8/7/15.
 */
public final class PnfsOperation implements Backgroundable {
    /*
     * Stored state. Instead of using enum, to leave less of a memory footprint.
     * As above.
     */
    public static final byte REPLICA   = 0;
    public static final byte OUTPUT    = 1;
    public static final byte CUSTODIAL = 2;

    /**
     * Stored state. Instead of using enum, to leave less of a memory footprint.
     * The map storing operation markers is expected to be very large.
     * Order is significant.
     */
    static final byte WAITING  = 0;     // NEXT TASK READY TO RUN
    static final byte RUNNING  = 1;     // TASK SUBMITTED TO THE EXECUTOR
    static final byte DONE     = 2;     // CURRENT TASK SUCCESSFULLY COMPLETED
    static final byte CANCELED = 3;     // CURRENT TASK WAS TERMINATED BY USER
    static final byte FAILED   = 4;     // CURRENT TASK FAILED WITH EXCEPTION
    static final byte VOID     = 5;     // NO FURTHER WORK NEEDS TO BE DONE
    static final byte ABORTED  = 6;     // CANNOT DO ANYTHING FURTHER
    static final byte UNINITIALIZED = 7;

    private static final String TO_STRING =
                    "%s (%s %s)(%s %s)(parent %s, count %s, retried %s)";
    private static final String TO_HISTORY_STRING =
                    "%s (%s %s)(%s %s)(parent %s, retried %s) %s";
    private static final String FORMAT_STR = "E MMM dd HH:mm:ss zzz yyyy";

    private static final DateFormat format;

    static {
        format = new SimpleDateFormat(FORMAT_STR);
        format.setLenient(false);
    }

    static String getFormattedDateFromMillis(long millis) {
        return format.format(new Date(millis));
    }

    private final PnfsId      pnfsId;
    private final long        size;
    private byte              retentionPolicy;
    private byte              selectionAction;
    private int               poolGroup;
    private Integer           storageUnit;
    private Integer           parent;
    private Integer           source;
    private Integer           target;
    private Integer           lastType;
    private long              lastUpdate;
    private byte              state;
    private short             opCount;
    private short             retried;
    private short             locations;
    private boolean           checkSticky;

    private Collection<Integer> tried;
    private ResilientFileTask task;
    private CacheException    exception;

    private long              inWait = 0;
    private long              inRun  = 0;
    private long              lastStateChange;

    PnfsOperation(PnfsId pnfsId, int pgroup, Integer sunit, byte action,
                  short opCount, long size) {
        this(pnfsId, opCount, size);
        selectionAction = action;
        poolGroup = pgroup;
        storageUnit = sunit;
        state = UNINITIALIZED;
    }

    PnfsOperation(PnfsId pnfsId, short opCount, long size) {
        this.pnfsId = pnfsId;
        this.opCount = opCount;
        retried = 0;
        this.size = size;
        state = UNINITIALIZED;
    }

    @VisibleForTesting
    public PnfsOperation(PnfsOperation operation) {
        this(operation.pnfsId, operation.poolGroup, operation.storageUnit,
                        operation.selectionAction, operation.opCount,
                        operation.size);
        lastUpdate = operation.lastUpdate;
        parent = operation.parent;
        exception = operation.exception;
        retentionPolicy = operation.retentionPolicy;
        retried = operation.retried;
        source = operation.source;
        state = operation.state;
        target = operation.target;
        task = operation.task;
        locations = operation.locations;
        if (operation.tried != null) {
            tried.addAll(operation.tried);
        }
    }

    /**
     * <p>This method fires and forgets a message to the source pool
     *    to set the system-owned sticky bit.</p>
     *
     * <p>While this may most of the time be a redundant operation, the
     *    rationale behind it is to prevent having to check the sticky
     *    bit during a pool scan to see whether the copy is a replica or
     *    merely a temporarily cached one (via an ad hoc p2p transfer).
     *    By forcing the system sticky bit, we ensure that resilient
     *    pools will never have simply cached copies.</p>
     *
     * <p>Checking/verifying requires a response, which means either a
     *    blocking call or yet another queue of messages to handle.
     *    Since this operation most of the time will not be crucial,
     *    an asynchronous best effort is the most efficient solution.</p>
     *
     * <p>Should the message/command fail for some reason, it will be
     *    recorded on the pool side.</p>
     */
    public void ensureSticky(PoolInfoMap poolInfoMap, CellStubFactory factory) {
        if (!checkSticky) {
            return;
        }

        String pool = poolInfoMap.getPool(source);

        factory.getPoolStub(pool)
               .send(new EnsureSystemStickyBitMessage(poolInfoMap.getPool(source),
                                                      pnfsId));
    }

    public CacheException getException() {
        return exception;
    }

    public long getLastUpdate() {
        return lastUpdate;
    }

    public short getLocations() {
        return locations;
    }

    public synchronized short getOpCount() {
        return opCount;
    }

    public Integer getParent() {
        return parent;
    }

    public String getPrincipalPool(PoolInfoMap map) {
        if (parent != null) {
            return map.getPool(parent);
        }

        if (source != null) {
            return map.getPool(source);
        }

        if (target != null) {
            return map.getPool(target);
        }

        return "UNDEFINED";
    }

    public PnfsId getPnfsId() {
        return pnfsId;
    }

    public int getPoolGroup() {
        return poolGroup;
    }

    public byte getRetentionPolicy() {
        return retentionPolicy;
    }

    public byte getSelectionAction() {
        return selectionAction;
    }

    public long getSize() { return size; }

    public Integer getSource() {
        return source;
    }

    public synchronized byte getState() {
        return state;
    }

    public Integer getStorageUnit() {
        return storageUnit;
    }

    public Integer getTarget() {
        return target;
    }

    public long getTimeInRun() { return inRun; }

    public long getTimeInWait() { return inWait; }

    public Set<Integer> getTried() {
        if (tried == null) {
            return Collections.EMPTY_SET;
        }
        return ImmutableSet.copyOf(tried);
    }

    @Override
    public synchronized void incrementCount() {
        lastUpdate = System.currentTimeMillis();
        ++opCount;
    }

    @Override
    public boolean isBackground() {
        return parent != null;
    }

    public synchronized void relay(PoolMigrationCopyFinishedMessage message) {
        if (task != null) {
            task.relayMessage(message);
        }
    }

    @VisibleForTesting
    public void setLastUpdate(Long lastUpdate) {
        this.lastUpdate = lastUpdate;
    }

    public void setLocations(short numLocations) {
        this.locations = numLocations;
    }

    public synchronized void setOpCount(short opCount) {
        this.opCount = opCount;
    }

    public void setSource(Integer source) {
        this.source = source;
    }

    public void setTarget(Integer target) {
        this.target = target;
    }

    public synchronized void submit() {
        if (task != null) {
            task.submit();
        }
    }

    public String toString() {
        return String.format(TO_STRING, getFormattedDateFromMillis(lastUpdate),
                             pnfsId, getRetentionPolicyName(), lastTypeName(),
                             getStateName(), parent == null ? "none" : parent,
                             opCount, retried);
    }

    public String toHistoryString() {
        return String.format(TO_HISTORY_STRING, getFormattedDateFromMillis(lastUpdate),
                        pnfsId, getRetentionPolicyName(), lastTypeName(),
                        getStateName(), parent == null ? "none" : parent,
                        retried, exception == null ? "" : new ExceptionMessage(exception));
    }

    void abortOperation() {
        synchronized( this) {
            updateState(ABORTED);
            opCount = 0;
        }

        lastUpdate = System.currentTimeMillis();
        source = null;
        target = null;
    }

    void addSourceToTriedLocations() {
        if (source == null) {
            return;
        }

        if (tried == null) {
            tried = new ArrayList<>();
        }
        tried.add(source);
    }

    void addTargetToTriedLocations() {
        if (target == null) {
            return;
        }

        if (tried == null) {
            tried = new ArrayList<>();
        }
        tried.add(target);
    }

    void cancelCurrent() {
        synchronized( this) {
            updateState(CANCELED);
            --opCount;
        }

        lastUpdate = System.currentTimeMillis();
        if (task != null) {
            task.cancel();
        }
        retried = 0;
    }

    String getRetentionPolicyName() {
        switch (retentionPolicy) {
            case OUTPUT:
                return "OUTPUT";
            case CUSTODIAL:
                return "CUSTODIAL";
            case REPLICA:
            default:
                return "REPLICA";
        }
    }

    short getRetried() {
        return retried;
    }

    String getStateName() {
        switch (state) {
            case WAITING:
                return "WAITING";
            case RUNNING:
                return "RUNNING";
            case DONE:
                return "DONE";
            case CANCELED:
                return "CANCELED";
            case FAILED:
                return "FAILED";
            case VOID:
                return "VOID";
            case ABORTED:
                return "ABORTED";
            case UNINITIALIZED:
                return "UNINITIALIZED";
        }

        throw new IllegalArgumentException("No such state: " + state);
    }

    Type getType() {
        if (task == null) {
            return Type.VOID;
        }
        return task.getType();
    }

    void incrementRetried() {
        ++retried;
    }

    void resetOperation() {
        synchronized( this) {
            updateState(WAITING);
            task = null;
        }
        exception = null;
        lastUpdate = System.currentTimeMillis();
    }

    void resetSourceAndTarget() {
        retried = 0;
        source = null;
        target = null;
    }

    void setVerifySticky(boolean checkSticky) {
        this.checkSticky = checkSticky;
    }

    void setLastType() {
        if (task != null) {
            lastType = task.getTypeValue();
        }
    }

    void setParentOrSource(Integer pool, boolean isParent) {
        if (isParent) {
            parent = pool;
        } else {
            source = pool;
        }
    }

    void setRetentionPolicy(String policy) {
        switch (policy) {
            case "REPLICA":
                retentionPolicy = REPLICA;
                break;
            case "OUTPUT":
                retentionPolicy = OUTPUT;
                break;
            case "CUSTODIAL":
                retentionPolicy = CUSTODIAL;
                break;
            default:
                throw new IllegalArgumentException("No such policy: " + policy);
        }
    }

    synchronized void setState(byte state) {
        updateState(state);
    }

    @VisibleForTesting
    void setState(String state) {
        switch (state) {
            case "WAITING":     updateState(WAITING);   break;
            case "RUNNING":     updateState(RUNNING);   break;
            case "DONE":        updateState(DONE);      break;
            case "CANCELED":    updateState(CANCELED);  break;
            case "FAILED":      updateState(FAILED);    break;
            case "VOID":        updateState(VOID);      break;
            case "ABORTED":     updateState(ABORTED);   break;
            case "UNINITIALIZED":
                throw new IllegalArgumentException("Cannot set "
                                + "operation to UNINITIALIZED.");
            default:
                throw new IllegalArgumentException("No such state: " + state);
        }
    }

    void setTask(ResilientFileTask task) {
        this.task = task;
    }

    void updateOperation(CacheException error) {
        if (error != null) {
            exception = error;
            setState(FAILED);
        } else {
            synchronized( this) {
                updateState(DONE);
                --opCount;
            }
            retried = 0;
        }
        lastUpdate = System.currentTimeMillis();
    }

    synchronized void voidOperation() {
        synchronized( this) {
            updateState(VOID);
            opCount = 0;
        }
        retried = 0;
        source = null;
        target = null;
        locations = 0;
        tried = null;
        lastUpdate = System.currentTimeMillis();
    }

    private synchronized String lastTypeName() {
        return lastType == null ? "" : Type.values()[lastType].toString();
    }

    private void updateState(byte state) {
        if (this.state == state) {
            return;
        }

        switch(this.state) {
            case WAITING:
                inWait += (System.currentTimeMillis() - lastStateChange);
                break;
            case RUNNING:
                inRun  += (System.currentTimeMillis() - lastStateChange);
                break;
            default:
                break;
        }

        this.state = state;
        lastStateChange = System.currentTimeMillis();
    }
}
