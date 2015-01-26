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

import java.util.Collection;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.dcache.cells.CellStub;
import org.dcache.namespace.replication.caches.PnfsInfoCache;
import org.dcache.namespace.replication.caches.PoolInfoCache;
import org.dcache.namespace.replication.caches.PoolManagerPoolInfoCache;
import org.dcache.namespace.replication.caches.PoolStatusCache;

/**
 * A convenience container for all the injected executors, caches and cell stubs.
 * This is mainly in order to unclutter the constructors of the various
 * tasks requiring these components.
 *
 * @author arossi
 */
public final class ReplicaManagerHub {
    /*
     * Shared utility
     */
    public final CollectionElementSelectionStrategy<String> randomSelector
                    = new CollectionElementSelectionStrategy<String>() {

        private Random random = new Random(System.currentTimeMillis());

        public String select(Collection<String> collection) {
            if (collection.isEmpty()) {
                return null;
            }

            int index = Math.abs(random.nextInt()) % collection.size();

            Iterator<String> it = collection.iterator();

            for (int i = 0; i < index - 1; i++) {
                it.next();
            }

            return it.next();
        }
    };

    /*
     * Thread pools (executor services).
     */
    private Executor poolGroupInfoTaskExecutor;
    private Executor pnfsInfoTaskExecutor;
    private Executor reductionTaskExecutor;
    private Executor poolStatusChangeTaskExecutor;
    private ScheduledExecutorService migrationTaskExecutor;

    /*
     * Cell stubs.
     */
    private CellStub poolManager;
    private CellStub pnfsManager;
    private CellStub pinManager;
    private CellStubFactory poolStubFactory;

    /*
     * Caches.
     */
    private PnfsInfoCache pnfsInfoCache;
    private PoolInfoCache poolInfoCache;
    private PoolManagerPoolInfoCache poolManagerPoolInfoCache;
    private PoolStatusCache poolStatusCache;

    /*
     * Worker settings.
     */
    private long poolStatusChangeWindow;
    private TimeUnit poolStatusChangeWindowUnit;
    private boolean isHandleRestartMessagesEnabled;

    public long getPoolStatusChangeWindow() {
        return poolStatusChangeWindow;
    }

    public void setPoolStatusChangeWindow(long poolStatusChangeWindow) {
        this.poolStatusChangeWindow = poolStatusChangeWindow;
    }

    public TimeUnit getPoolStatusChangeWindowUnit() {
        return poolStatusChangeWindowUnit;
    }

    public void setPoolStatusChangeWindowUnit(
                    TimeUnit poolStatusChangeWindowUnit) {
        this.poolStatusChangeWindowUnit = poolStatusChangeWindowUnit;
    }

    public boolean isHandleRestartMessagesEnabled() {
        return isHandleRestartMessagesEnabled;
    }

    public void setHandleRestartMessagesEnabled(
                    boolean isHandleRestartMessagesEnabled) {
        this.isHandleRestartMessagesEnabled = isHandleRestartMessagesEnabled;
    }

    public PnfsInfoCache getPnfsInfoCache() {
        return pnfsInfoCache;
    }

    public void setPnfsInfoCache(PnfsInfoCache pnfsInfoCache) {
        this.pnfsInfoCache = pnfsInfoCache;
    }

    public PoolInfoCache getPoolInfoCache() {
        return poolInfoCache;
    }

    public void setPoolInfoCache(PoolInfoCache poolInfoCache) {
        this.poolInfoCache = poolInfoCache;
    }

    public PoolManagerPoolInfoCache getPoolManagerPoolInfoCache() {
        return poolManagerPoolInfoCache;
    }

    public void setPoolManagerPoolInfoCache(PoolManagerPoolInfoCache poolManagerPoolInfoCache) {
        this.poolManagerPoolInfoCache = poolManagerPoolInfoCache;
    }

    public PoolStatusCache getPoolStatusCache() {
        return poolStatusCache;
    }

    public void setPoolStatusCache(PoolStatusCache poolStatusCache) {
        this.poolStatusCache = poolStatusCache;
    }

    public ScheduledExecutorService getMigrationTaskExecutor() {
        return migrationTaskExecutor;
    }

    public void setMigrationTaskExecutor(ScheduledExecutorService executor) {
        migrationTaskExecutor = executor;
    }

    public CellStub getPinManager() {
        return pinManager;
    }

    public void setPinManager(CellStub pinManager) {
        this.pinManager = pinManager;
    }

    public Executor getPnfsInfoTaskExecutor() {
        return pnfsInfoTaskExecutor;
    }

    public void setPnfsInfoTaskExecutor(Executor executor) {
        pnfsInfoTaskExecutor = executor;
    }

    public CellStub getPnfsManager() {
        return pnfsManager;
    }

    public void setPnfsManager(CellStub pnfsManager) {
        this.pnfsManager = pnfsManager;
    }

    public Executor getPoolGroupInfoTaskExecutor() {
        return poolGroupInfoTaskExecutor;
    }

    public void setPoolGroupInfoTaskExecutor(Executor executor) {
        poolGroupInfoTaskExecutor = executor;
    }

    public CellStub getPoolManager() {
        return poolManager;
    }

    public void setPoolManager(CellStub poolManager) {
        this.poolManager = poolManager;
    }

    public Executor getPoolStatusChangeTaskExecutor() {
        return poolStatusChangeTaskExecutor;
    }

    public void setPoolStatusChangeTaskExecutor(Executor executor) {
        poolStatusChangeTaskExecutor = executor;
    }

    public CellStubFactory getPoolStubFactory() {
        return poolStubFactory;
    }

    public void setPoolStubFactory(CellStubFactory poolStubFactory) {
        this.poolStubFactory = poolStubFactory;
    }

    public Executor getReductionTaskExecutor() {
        return reductionTaskExecutor;
    }

    public void setReductionTaskExecutor(Executor executor) {
        reductionTaskExecutor = executor;
    }
}
