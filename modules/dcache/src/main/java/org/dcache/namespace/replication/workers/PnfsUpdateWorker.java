package org.dcache.namespace.replication.workers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.ExecutionException;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.PnfsId;
import org.dcache.namespace.replication.ReplicaManagerHub;
import org.dcache.namespace.replication.data.PoolGroupInfo;
import org.dcache.vehicles.FileAttributes;

/**
 * Created by arossi on 1/22/15.
 */
public class PnfsUpdateWorker implements Runnable {
    private static final Logger LOGGER
                    = LoggerFactory.getLogger(PnfsUpdateWorker.class);

    enum Phase {
        POOLGROUPINFO, FILEINFO, CACHEENTRYINFO, MIGRATION, REDUCTION, DONE
    }

    class CacheEntryResultListener implements Runnable {
        public void run() {
            CacheEntryInfoMessage message = null;
            try {
                message = future.get();
            } catch (InterruptedException | ExecutionException t) {
                hub.getFileInfoTaskHandler()
                                .taskFailed(message, t.getMessage());
                return;
            }

            if (future.isCancelled()) {
                hub.getFileInfoTaskHandler()
                                .taskCancelled(message, "Future task was cancelled");
            } else {
                hub.getFileInfoTaskHandler()
                                .taskCompleted(message, tried);
            }
        }
    }

    private final String pool;
    private final PnfsId pnfsId;
    private final ReplicaManagerHub hub;

    private PoolGroupInfo poolGroupInfo;
    private Set<String> triedPools;
    private FileAttributes attributes;
    private ListenableFuture<CacheEntryInfoMessage> future;

    /*
    * State.
    */
    private Phase phase = Phase.POOLGROUPINFO;

    public PnfsUpdateWorker(String pool, PnfsId pnfsId, ReplicaManagerHub hub) {
        this.pool = pool;
        this.pnfsId = pnfsId;
        this.hub = hub;
    }

    public void launch() {
        LOGGER.debug("launching phase {} for {} on {}.", phase, pnfsId, pool);

        switch (phase) {
            case POOLGROUPINFO:
                hub.getPoolGroupInfoTaskExecutor().execute(this);
                break;
            case FILEINFO:
                hub.getPnfsInfoTaskExecutor().execute(this);
                break;
            case CACHEENTRYINFO:
                run();
                break;
            case MIGRATION:

                break;
            case REDUCTION:

                break;
            case DONE:
                return;
            default:
                String message = String.format("worker for %s on %s in an "
                                                + "unknown state %s.",
                                                pnfsId, pool, phase);
                throw new IllegalStateException(message);
        }
    }

    private void nextPhase() {
        switch (phase) {
            case POOLGROUPINFO:
                phase = Phase.FILEINFO;
                break;
            case FILEINFO:
                phase = Phase.CACHEENTRYINFO;
                break;
            case CACHEENTRYINFO:
                phase = Phase.MIGRATION;
                break;
            case MIGRATION:
                phase = Phase.REDUCTION;
                break;
            case REDUCTION:
                done();
                break;
            case DONE:
            default:
                break;
        }

        launch();
    }

    private void done() {
        phase = Phase.DONE;
        LOGGER.debug("completed processing of {} on {}.", pnfsId, pool);
    }

    public void setTriedPools(Set<String> triedPools) {
        this.triedPools = triedPools;
    }

    @Override
    public void run() {
        switch (phase) {
            case POOLGROUPINFO:
                getPoolGroupInfo();
                break;
            case FILEINFO:
                getFileInfo();
                break;
            case CACHEENTRYINFO:
                getCacheEntryInfo();
                break;
            case MIGRATION:
                doMigration();
                break;
            case REDUCTION:
                doReduction();
                break;
            case DONE:
            default:
                break;
        }

        LOGGER.debug("completed phase {} for {} on {}.", phase, pnfsId, pool);

        nextPhase();
    }

    private void getPoolGroupInfo() {
        try {
            poolGroupInfo = hub.getPoolInfoCache().getPoolGroupInfo(pool);
            if (!poolGroupInfo.isResilient()) {
                LOGGER.debug("{} does not belong to a resilient group", pool);
                done();
                return;
            }
        } catch (ExecutionException t) {
            /*
             *   TODO:  We may want to change this policy to an immediate retry.
             */
            LOGGER.error("Verifying pool group information for {}@{} failed: {}, {}.  "
                          + "Replication cannot proceed at this time, but will "
                          + "be retried during the next periodic watchdog scan.",
                pnfsId, pool, t.getMessage(), String.valueOf(t.getCause()));
            done();
        }
    }

    private void getFileInfo() {
        try {
            attributes = hub.getPnfsInfoCache().getAttributes(pnfsId);
            if (!attributes.getAccessLatency().equals(AccessLatency.ONLINE)) {
                LOGGER.debug("AccessLatency of {} is not ONLINE; ignoring ...",
                                pnfsId);
                done();
                return;
            }
        } catch (ExecutionException t) {
            LOGGER.error("CacheEntryInfoTask failed for %{}@%{}, {}, cause {}.",
                            pnfsId, pool, t.getMessage(), String.valueOf(t.getCause()));
            done();
            return;
        }
    }

    private void getCacheEntryInfo() {
        future = hub.getPoolStubFactory()
                        .getCellStub(pool)
                        .send(new CacheEntryInfoMessage(pnfsId));
        future.addListener(new CacheEntryResultListener(),
                           hub.getPnfsInfoTaskExecutor());
        LOGGER.trace("Sent CacheEntryInfoMessage for {} to {}". pnfsId, pool);
    }

    private void doMigration() {

    }

    private void doReduction() {

    }
}
