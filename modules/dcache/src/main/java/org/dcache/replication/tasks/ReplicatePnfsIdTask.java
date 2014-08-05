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

import com.google.common.base.Preconditions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import diskCacheV111.util.PnfsId;

import org.dcache.alarms.AlarmMarkerFactory;
import org.dcache.alarms.Severity;
import org.dcache.cells.CellStub;
import org.dcache.replication.api.PnfsCacheMessageType;
import org.dcache.replication.api.ReplicationCopyMessageFactory;
import org.dcache.replication.api.ReplicationEndpoints;
import org.dcache.replication.api.ReplicationOperationRegistry;
import org.dcache.replication.api.ReplicationQueryUtilities;
import org.dcache.replication.api.ReplicationStatistics;
import org.dcache.replication.api.ReplicationTaskExecutor;
import org.dcache.replication.data.PnfsIdMetadata;

/**
 * Calls out to the pool migration module with a request for the additional
 * copies.
 *
 * XXX This module will be modified to accommodate the changes to the
 *     migration module once they are in place. XXX
 *
 * @author arossi
 */
public final class ReplicatePnfsIdTask extends PnfsIdRequestTask {
    private final ReplicationCopyMessageFactory factory;

    public ReplicatePnfsIdTask(PnfsIdMetadata opData,
                               ReplicationEndpoints hub,
                               ReplicationQueryUtilities utils,
                               ReplicationOperationRegistry map,
                               ReplicationStatistics statistics,
                               ReplicationCopyMessageFactory factory) {
        super(opData, hub, utils, map, statistics);
        this.factory = Preconditions.checkNotNull(factory);
    }

    public Void call() throws Exception {
        List<String> jobs = new ArrayList<>();
        int limit = opData.getAbsoluteDelta();

        String sourcePool;
        if (opData.sourceType.equals(PnfsCacheMessageType.CLEAR)) {
            sourcePool = utils.removeRandomEntry(opData.getReplicaPools());
        } else {
            sourcePool = opData.poolName;
        }

        if (sourcePool == null) {
            LOGGER.error(AlarmMarkerFactory.getMarker(Severity.HIGH,
                                                      PnfsIdMetadata.ALARM_INACCESSIBLE,
                                                      opData.poolName),
                            "The only copy of {} has been removed from {}; "
                            + "this file must be recovered manually.",
                            opData.pnfsId,
                            opData.poolName);
            map.unregister(opData);
            return null;
        }

        String pnfsid = opData.pnfsId.toString();
        String poolGroup = opData.poolGroupData.poolGroup.getName();
        boolean sameHostOK = opData.poolGroupData.constraints.isSameHostOK();

        /*
         * XXX The loop will hopefully disappear when the migration module
         * is modified to support multiple copy requests for a single pnfsid
         * processed by a permanent migration job.
         */
        for (int copy = 0; copy < limit; ++copy) {
            String jobId = requestCopies(opData.pnfsId,
                                         sourcePool,
                                         poolGroup,
                                         1, // for now, singletons
                                         sameHostOK);
            if (jobId != null) {
                jobs.add(jobId);
                statistics.increment(ReplicationTaskExecutor.TASKS,
                                     ReplicationTaskExecutor.REPLICATE);
            }
        }

        if (jobs.isEmpty()) {
            LOGGER.info("pnfsid {}, no replicas requested at this time", pnfsid);
            map.unregister(opData);
        } else {
            /*
             * XXX Purely for informational purposes.  As migration copy gets
             * modified, the job id will always be the same, so this
             * part of the code will also disappear.
             */
            LOGGER.info("pnfsid {}, replica request mapped to {}", pnfsid,
                                                                   jobs);
        }

        return null;
    }

    /**
     * <ol><li>Request the copy from the target.</li>
     *     <li>Wait for reply.</li>
     *     <li>Extract id from reply.</li></ol>
     */
    private String requestCopies(PnfsId pnfsId,
                                 String pool,
                                 String poolGroup,
                                 int numCopies,
                                 boolean sameHostOK) {
        CellStub stub = factory.getTargetStub(pool);
        stub.setCellEndpoint(hub.getEndpoint());
        Serializable message = factory.createReplicateRequestMessage(pnfsId,
                                                                     poolGroup,
                                                                     numCopies,
                                                                     sameHostOK);
        Object reply;
        try {
            reply = stub.sendAndWait(message, factory.getMessageClass());
            if( reply == null ) {
                LOGGER.warn("Copy request {} timed out.", message);
                statistics.incrementFailed(ReplicationTaskExecutor.TASKS,
                                           ReplicationTaskExecutor.REPLICATE);
                return null;
            }
        } catch (Exception ce ) {
            statistics.incrementFailed(ReplicationTaskExecutor.TASKS,
                                       ReplicationTaskExecutor.REPLICATE);
            factory.handleException(ce, message);
            return null;
        }

        message = (Serializable)reply;
        return factory.extractRequestIdFromReply(message);
    }
}
