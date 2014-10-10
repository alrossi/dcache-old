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
package org.dcache.replication.tasks;

import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.PnfsId;

import org.dcache.alarms.AlarmMarkerFactory;
import org.dcache.alarms.PredefinedAlarm;
import org.dcache.cells.CellStub;
import org.dcache.pool.migration.CacheEntryMode;
import org.dcache.pool.migration.JobDefinition;
import org.dcache.pool.migration.Task;
import org.dcache.pool.migration.TaskCompletionHandler;
import org.dcache.pool.repository.CacheEntry;
import org.dcache.pool.repository.EntryState;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.replication.api.PnfsCacheMessageType;
import org.dcache.replication.api.ReplicationEndpoints;
import org.dcache.replication.api.ReplicationOperationRegistry;
import org.dcache.replication.api.ReplicationQueryUtilities;
import org.dcache.replication.api.ReplicationStatistics;
import org.dcache.replication.api.ReplicationTaskExecutor;
import org.dcache.replication.data.PnfsIdMetadata;
import org.dcache.replication.data.ReplicaJobDefinition;
import org.dcache.vehicles.FileAttributes;

/**
 * Calls out to the pool migration module by executing a migration Task.
 * Acts as the completion handler for the migration task, registering
 * the result with the map.
 *
 * @author arossi
 */
public final class ReplicatePnfsIdTask extends PnfsIdRequestTask
                                       implements TaskCompletionHandler {
    private static final List<StickyRecord> SYSTEM_STICKY
        = ImmutableList.of(new StickyRecord("system", StickyRecord.NON_EXPIRING));

    class SourceCacheEntry implements CacheEntry {
        private final PnfsId pnfsId;
        private final FileAttributes attributes;

        SourceCacheEntry(PnfsId pnfsId, List<String> locations) {
            this.pnfsId = pnfsId;
            attributes = new FileAttributes();
            attributes.setAccessLatency(AccessLatency.ONLINE);
            attributes.setLocations(locations);
        }

        public PnfsId getPnfsId() {
            return pnfsId;
        }

        public long getReplicaSize() {
            return 0;
        }

        public FileAttributes getFileAttributes() {
            return attributes;
        }

        public EntryState getState() {
            return EntryState.CACHED;
        }

        public long getCreationTime() {
            return 0;
        }

        public long getLastAccessTime() {
            return 0;
        }

        public int getLinkCount() {
            return 0;
        }

        public boolean isSticky() {
            return true;
        }

        public Collection<StickyRecord> getStickyRecords() {
            return SYSTEM_STICKY;
        }
    }

    public ReplicatePnfsIdTask(PnfsIdMetadata opData,
                               ReplicationEndpoints hub,
                               ReplicationQueryUtilities utils,
                               ReplicationOperationRegistry map,
                               ReplicationStatistics statistics) {
        super(opData, hub, utils, map, statistics);
    }

    public Void call() throws Exception {
        String sourcePool;
        if (opData.getSourceType().equals(PnfsCacheMessageType.CLEAR)) {
            sourcePool = utils.removeRandomEntry(opData.getReplicaPools());
        } else {
            sourcePool = opData.poolName;
        }

        if (sourcePool == null) {
            LOGGER.error(AlarmMarkerFactory.getMarker(PredefinedAlarm.INACCESSIBLE_FILE,
                                                      opData.pnfsId.toString(),
                                                      opData.poolName),
                            "The only copy of {} has been removed from {}; "
                            + "this file must be recovered manually.",
                            opData.pnfsId,
                            opData.poolName);
            map.unregister(opData);
            return null;
        }

        CacheEntryMode mode
            = new CacheEntryMode(CacheEntryMode.State.CACHED, SYSTEM_STICKY);

        JobDefinition mjobDefinition
            = new ReplicaJobDefinition(sourcePool,
                                       opData.poolGroupData.poolGroup.getName(),
                                       hub,
                                       mode,
                                       opData.getAbsoluteDelta(),
                                       opData.poolGroupData.constraints.isSameHostOK());

        CellStub stub = new CellStub();
        stub.setCellEndpoint(hub.getEndpoint());

        ScheduledExecutorService executor = null;

        new Task(this,
                 stub,
                 hub.getPnfsManager(),
                 null,
                 executor,
                 sourcePool,
                 new SourceCacheEntry(opData.pnfsId,
                                      opData.getReplicaPools()),
                 mjobDefinition).run();

        statistics.increment(ReplicationTaskExecutor.TASKS,
                             ReplicationTaskExecutor.REPLICATE,
                             opData.getAbsoluteDelta());

        LOGGER.info("pnfsid {}, replica task running for {} on {}",
                     opData.pnfsId, opData.getAbsoluteDelta(), sourcePool);

        return null;
    }

    public void taskCancelled(Task task) {
        // TODO Auto-generated method stub

    }

    public void taskFailed(Task task, String msg) {
        // TODO Auto-generated method stub

    }

    public void taskFailedPermanently(Task task, String msg) {
        // TODO Auto-generated method stub

    }

    public void taskCompleted(Task task) {

    }
}
