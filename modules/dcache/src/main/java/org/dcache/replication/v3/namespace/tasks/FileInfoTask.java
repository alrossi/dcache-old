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
package org.dcache.replication.v3.namespace.tasks;

import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.ExecutionException;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.PnfsId;

import org.dcache.cells.CellStub;
import org.dcache.replication.v3.CDCFixedPoolTaskExecutor;
import org.dcache.replication.v3.namespace.ResilientInfoCache;
import org.dcache.replication.v3.namespace.handlers.task.FileInfoTaskCompletionHandler;
import org.dcache.replication.v3.vehicles.CacheEntryInfoMessage;
import org.dcache.vehicles.FileAttributes;

/**
 * Fetches the cache entry info and handles the asynchronous reply from
 * the pool repository by passing off the message to a completion handler.
 * <p>
 * Note that if the source file access latency is not for some reason
 * ONLINE, the task returns without doing further work.
 *
 * @author arossi
 */
public class FileInfoTask implements Runnable {
    private static final Logger LOGGER
        = LoggerFactory.getLogger(FileInfoTask.class);

    class CacheEntryResultListener implements Runnable {
        public void run() {
            CacheEntryInfoMessage message = null;
            try {
                message = future.get();
            } catch (InterruptedException | ExecutionException t) {
                handler.taskFailed(message, t.getMessage());
                return;
            }

            if (future.isCancelled()) {
                handler.taskCancelled(message, "Future task was cancelled");
            } else {
                handler.taskCompleted(message, tried);
            }
        }
    }

    private final PnfsId pnfsId;
    private final CellStub pool;
    private final ResilientInfoCache cache;
    private final FileInfoTaskCompletionHandler handler;
    private final CDCFixedPoolTaskExecutor executor;
    private final Set<String> tried;
    private ListenableFuture<CacheEntryInfoMessage> future;

    public FileInfoTask(PnfsId pnfsId,
                        CellStub pool,
                        FileInfoTaskCompletionHandler handler,
                        ResilientInfoCache cache,
                        CDCFixedPoolTaskExecutor executor,
                        Set<String> tried) {
        this.pnfsId = pnfsId;
        this.pool = pool;
        this.handler = handler;
        this.cache = cache;
        this.executor = executor;
        this.tried = tried;
    }

    public void run() {
        FileAttributes attributes;
        try {
            attributes = cache.getAttributes(pnfsId);
            if (!attributes.getAccessLatency().equals(AccessLatency.ONLINE)) {
                handler.taskCancelled(null, pnfsId
                                            + " is not ONLINE; ignoring ...");
                return;
            }
        } catch (ExecutionException t) {
            String error = "CacheEntryInfoTask failed "
                            + "for " + pnfsId + "@" +
                            pool.getDestinationPath().getCellName();
            handler.taskFailed(null, error);
            return;
        }

        future = pool.send(new CacheEntryInfoMessage(pnfsId));
        future.addListener(new CacheEntryResultListener(), executor);
        LOGGER.trace("Sent CacheEntryInfoMessage for {} to {}",
                        pnfsId,
                        pool.getDestinationPath().getCellName());
    }
}
