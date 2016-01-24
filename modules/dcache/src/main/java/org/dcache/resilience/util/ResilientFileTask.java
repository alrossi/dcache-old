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
package org.dcache.resilience.util;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.PnfsId;
import org.dcache.pool.classic.Cancellable;
import org.dcache.pool.migration.PoolMigrationCopyFinishedMessage;
import org.dcache.pool.migration.Task;
import org.dcache.resilience.handlers.PnfsOperationHandler;
import org.dcache.resilience.handlers.PnfsOperationHandler.Type;
import org.dcache.vehicles.FileAttributes;

/**
 * <p>Main wrapper task for calling {@link PnfsOperationHandler}.</p>
 *
 * <p>First runs a verification on the pnfsId to see what kind of
 *      operation is required.  It then proceeds with either a single copy,
 *      remove, or NOP.  The operation is cancellable through its
 *      {@link Future}.</p>
 *
 * <p>In the case of a copy/migration operation, the completion message
 *      from the pool is relayed by this task object to the internal migration
 *      task object.</p>
 *
 * Created by arossi on 8/6/15.
 */
public final class ResilientFileTask implements Cancellable, Callable<Void> {
    private final PnfsId               pnfsId;
    private final PnfsOperationHandler handler;

    private final boolean suppressAlarm;
    private Task   migrationTask;
    private Future future;
    private Type   type;

    public ResilientFileTask(PnfsId pnfsId, boolean suppressAlarm,
                             PnfsOperationHandler handler) {
        this.pnfsId = pnfsId;
        this.handler = handler;
        this.suppressAlarm = suppressAlarm;
    }

    @Override
    public Void call() {
        FileAttributes attributes = new FileAttributes();
        attributes.setAccessLatency(AccessLatency.ONLINE);
        attributes.setPnfsId(pnfsId);

        type = handler.handleVerification(attributes, suppressAlarm);

        switch (type) {
            case VOID:
                break;
            case COPY:
                Task innerTask = handler.handleMakeOneCopy(attributes);
                if (innerTask != null) {
                    MessageGuard.setResilienceSession();
                    this.migrationTask = innerTask;
                    innerTask.run();
                }
                break;
            case REMOVE:
                MessageGuard.setResilienceSession();
                handler.handleRemoveOneCopy(attributes);
                break;
        }

        return null;
    }

    @Override
    public void cancel() {
        if (migrationTask != null) {
            migrationTask.cancel();
        }

        if (future != null) {
            future.cancel(true);
        }
    }

    public Task getMigrationTask() {
        return migrationTask;
    }

    public Integer getTypeValue() {
        if (type == null) return null;
        return type.ordinal();
    }

    public Type getType() {
        return type;
    }

    public void relayMessage(PoolMigrationCopyFinishedMessage message) {
        if (!message.getPnfsId().equals(pnfsId)) {
            return;
        }

        if (migrationTask == null) {
            String msg = String.format("Pool migration copy finished message "
                                        + "arrived for %s, but migration task "
                                        + "object has already been removed.",
                                       pnfsId);
            throw new IllegalStateException(msg);
        }

        migrationTask.messageArrived(message);
    }

    public void submit() {
        future = this.handler.getTaskService().submit(new FutureTask<>(this));
    }
}
