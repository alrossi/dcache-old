package org.dcache.replication.v3.namespace.handlers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dcache.replication.v3.CellStubFactory;
import org.dcache.replication.v3.ReplicaManagerTaskExecutor;
import org.dcache.replication.v3.namespace.ResilientInfoRegistry;
import org.dcache.replication.v3.namespace.tasks.ReplicaJobDefinition;
import org.dcache.replication.v3.namespace.tasks.ResilientFileInfoTask;
import org.dcache.replication.v3.vehicles.ResilientPoolInfoForPnfsId;
import org.dcache.replication.v3.vehicles.ResilientPoolInfo;

/**
 * @author arossi
 */
public class ResilientPoolInfoHandler {
    protected static final Logger LOGGER
        = LoggerFactory.getLogger(ResilientPoolInfoHandler.class);

    private ResilientInfoRegistry registry;
    private CellStubFactory poolStubFactory;
    private ReplicaManagerTaskExecutor pnfsidQueryExecutor;
    private ReplicaManagerTaskExecutor poolQueryExecutor;
    private ReplicaManagerTaskExecutor migrationTaskExecutor;
    private ReplicaManagerTaskExecutor reductionTaskExecutor;

    public  void messageArrived(ResilientPoolInfo message) {
        LOGGER.trace("ResilientPoolInfoMessage message arrived for {}.",
                        message.poolName);

        if (!message.isResilient()) {
            LOGGER.debug("{} does not belong to a resilient pool group; "
                            + "skipping ...",
                            message.poolName);
            return;
        }

        /*
         * This message was returned subsequent to a pool info request issued in
         * connection with a pool status change message. We now want to retrieve
         * a list, based on the state change, of files on the pool which are
         * either in need of another copy or have too many copies.
         *
         * Each pnfsid on the list is then queued, depending on its
         * requirements, on the migration or reduction executor.
         */
    }

    public void setRegistry(ResilientInfoRegistry registry) {
        this.registry = registry;
    }

    public void setPnfsidQueryExecutor(
                    ReplicaManagerTaskExecutor pnfsidQueryExecutor) {
        this.pnfsidQueryExecutor = pnfsidQueryExecutor;
    }

    public void setPoolQueryExecutor(ReplicaManagerTaskExecutor poolQueryExecutor) {
        this.poolQueryExecutor = poolQueryExecutor;
    }

    public void setPoolStubFactory(CellStubFactory poolStubFactory) {
        this.poolStubFactory = poolStubFactory;
    }

    public void setMigrationTaskExecutor(
                    ReplicaManagerTaskExecutor migrationTaskExecutor) {
        this.migrationTaskExecutor = migrationTaskExecutor;
    }

    public void setReductionTaskExecutor(
                    ReplicaManagerTaskExecutor reductionTaskExecutor) {
        this.reductionTaskExecutor = reductionTaskExecutor;
    }
}
