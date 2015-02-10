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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.dcache.namespace.replication.caches.PoolInfoCache;
import org.dcache.namespace.replication.caches.PoolManagerPoolInfoCache;
import org.dcache.namespace.replication.caches.PoolStatusCache;
import org.dcache.namespace.replication.db.LocalNamespaceAccess;
import org.dcache.namespace.replication.monitoring.ActivityRegistry;
import org.dcache.util.replication.CellStubFactory;

/**
 * A convenience container for all the injected executors, caches and services.
 * This is mainly in order to unclutter the constructors of the various
 * tasks requiring these components.
 *
 * Created by arossi on 1/25/15.
 */
public final class ReplicationHub {
    /**
     *  A shared static utility.
     */
    public static String exceptionMessage(Exception e) {
        Throwable t = e.getCause();
        return String.format("Exception: %s%s.", e.getMessage(),
                        t == null ? "" : String.format(", cause: %s", t));
    }

    /*
     * For attribute information and location queries.
     */
    private LocalNamespaceAccess access;

    /*
     * Monitoring
     */
    private ActivityRegistry registry;

    /*
     * Thread pools (executor services).
     */
    private ExecutorService verifyPoolExecutor;
    private ExecutorService processPnfsIdExecutor;
    private ExecutorService processPoolExecutor;
    private ExecutorService makeCopiesExecutor;
    private ExecutorService removeExtrasExecutor;
    private ScheduledExecutorService migrationTaskExecutor;

    /*
     * Cell stubs.
     */
    private CellStubFactory cellStubFactory;

    /*
     * Caches.
     */
    private PoolInfoCache poolInfoCache;
    private PoolManagerPoolInfoCache poolManagerPoolInfoCache;
    private PoolStatusCache poolStatusCache;

    /*
     * Sentinel settings.
     */
    private long poolStatusGracePeriod;
    private TimeUnit poolStatusGracePeriodUnit;

    public LocalNamespaceAccess getAccess() {
        return access;
    }

    public void setAccess(LocalNamespaceAccess access) {
        this.access = access;
    }

    public ActivityRegistry getRegistry() {
        return registry;
    }

    public void setRegistry(ActivityRegistry registry) {
        this.registry = registry;
    }

    public ExecutorService getVerifyPoolExecutor() {
        return verifyPoolExecutor;
    }

    public void setVerifyPoolExecutor(ExecutorService verifyPoolExecutor) {
        this.verifyPoolExecutor = verifyPoolExecutor;
    }

    public ExecutorService getProcessPnfsIdExecutor() {
        return processPnfsIdExecutor;
    }

    public void setProcessPnfsIdExecutor(
                    ExecutorService processPnfsIdExecutor) {
        this.processPnfsIdExecutor = processPnfsIdExecutor;
    }

    public ExecutorService getProcessPoolExecutor() {
        return processPoolExecutor;
    }

    public void setProcessPoolExecutor(ExecutorService processPoolExecutor) {
        this.processPoolExecutor = processPoolExecutor;
    }

    public ExecutorService getMakeCopiesExecutor() {
        return makeCopiesExecutor;
    }

    public void setMakeCopiesExecutor(ExecutorService makeCopiesExecutor) {
        this.makeCopiesExecutor = makeCopiesExecutor;
    }

    public ExecutorService getRemoveExtrasExecutor() {
        return removeExtrasExecutor;
    }

    public void setRemoveExtrasExecutor(ExecutorService removeExtrasExecutor) {
        this.removeExtrasExecutor = removeExtrasExecutor;
    }

    public ScheduledExecutorService getMigrationTaskExecutor() {
        return migrationTaskExecutor;
    }

    public void setMigrationTaskExecutor(
                    ScheduledExecutorService migrationTaskExecutor) {
        this.migrationTaskExecutor = migrationTaskExecutor;
    }

    public CellStubFactory getCellStubFactory() {
        return cellStubFactory;
    }

    public void setCellStubFactory(CellStubFactory cellStubFactory) {
        this.cellStubFactory = cellStubFactory;
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

    public void setPoolManagerPoolInfoCache(
                    PoolManagerPoolInfoCache poolManagerPoolInfoCache) {
        this.poolManagerPoolInfoCache = poolManagerPoolInfoCache;
    }

    public PoolStatusCache getPoolStatusCache() {
        return poolStatusCache;
    }

    public void setPoolStatusCache(PoolStatusCache poolStatusCache) {
        this.poolStatusCache = poolStatusCache;
    }

    public long getPoolStatusGracePeriod() {
        return poolStatusGracePeriod;
    }

    public void setPoolStatusGracePeriod(long poolStatusGracePeriod) {
        this.poolStatusGracePeriod = poolStatusGracePeriod;
    }

    public TimeUnit getPoolStatusGracePeriodUnit() {
        return poolStatusGracePeriodUnit;
    }

    public void setPoolStatusGracePeriodUnit(
                    TimeUnit poolStatusGracePeriodUnit) {
        this.poolStatusGracePeriodUnit = poolStatusGracePeriodUnit;
    }
}
