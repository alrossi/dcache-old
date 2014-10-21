package org.dcache.replication.v3.namespace.tasks;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import diskCacheV111.util.PnfsId;

import org.dcache.cells.CellStub;
import org.dcache.replication.v3.namespace.ResilientInfoCache;
import org.dcache.replication.v3.namespace.handlers.task.ReductionTaskCompletionHandler;

/**
 * @author arossi
 *
 */
public class ReductionTask implements Runnable {

    private final PnfsId pnfsId;
    private final ResilientInfoCache cache;
    private final CellStub poolManager;
    private final ReductionTaskCompletionHandler completionHandler;
    private final int numberOfCopies;
    private final Set<String> allLocations;
    private final Set<String> confirmedLocations;

    public ReductionTask(PnfsId pnfsId,
                         Collection<String> confirmedLocations,
                         ResilientInfoCache cache,
                         ReductionTaskCompletionHandler completionHandler) {
        this(pnfsId, confirmedLocations.size(), cache, null, completionHandler);
        this.confirmedLocations.addAll(confirmedLocations);
    }

    public ReductionTask(PnfsId pnfsId,
                         int numberOfCopies,
                         ResilientInfoCache cache,
                         CellStub poolManager,
                         ReductionTaskCompletionHandler completionHandler) {
        this.pnfsId = pnfsId;
        this.cache = cache;
        this.poolManager = poolManager;
        this.completionHandler = completionHandler;
        this.numberOfCopies = numberOfCopies;
        allLocations = new HashSet<>();
        confirmedLocations = new HashSet<>();
    }

    public void run() {
        if (confirmedLocations == null) {
            reduceByPoolCost();
        } else {
            eliminateUnconfirmed();
        }

    }

    private void eliminateUnconfirmed() {
        // TODO Auto-generated method stub

    }

    private void reduceByPoolCost() {
        // TODO Auto-generated method stub

    }
}
