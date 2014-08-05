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
package org.dcache.replication.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPoolGroup;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PnfsModifyCacheLocationMessage;

import org.dcache.alarms.AlarmMarkerFactory;
import org.dcache.alarms.Severity;
import org.dcache.replication.api.ReplicationOperationMode;
import org.dcache.replication.api.ReplicationOperationRegistry;
import org.dcache.replication.api.ReplicationQueryUtilities;
import org.dcache.replication.data.PnfsIdMetadata;
import org.dcache.vehicles.replication.ReplicationStatusMessage;

/**
 * Tracks in-flight PnfsId operations (reduce, replicate).
 * The {@link #update(ReplicationStatusMessage)}
 * and {@link #update(PnfsIdMetadata, PnfsModifyCacheLocationMessage)} methods
 * automatically remove an operation from the map if it is in a terminal state.
 *
 * @author arossi
 */
public final class MapOperationRegistry implements ReplicationOperationRegistry {
    private static final Logger LOGGER
        = LoggerFactory.getLogger(MapOperationRegistry.class);
    private static final String SEPARATOR
        = "\n_________________________________________________________________\n";

    private final Map<String, PnfsIdMetadata> registry = new HashMap<>();

    private ReplicationQueryUtilities utils;

    public synchronized void clear() {
        registry.clear();
    }

    public String getOperationTokenKey(PnfsId pnfsId,
                                       SelectionPoolGroup poolGroup) {
        if (poolGroup == null) {
            return null;
        }
        return getOperationTokenKey(pnfsId.toString(), poolGroup.getName());
    }

    public String getOperationTokenKey(String pnfsId, String poolGroup) {
        return poolGroup + "." + pnfsId;
    }

    public synchronized boolean isRegistered(PnfsIdMetadata opData) {
        String key = getKey(opData);
        if (key == null) {
            return false;
        }
        return registry.containsKey(key);
    }

    /**
     *  Extracts from the registry all pools and pnsfids currently
     *  involved in operations of the given mode.
     *
     *  @param mode to match (either REPLICATE or REDUCE).
     */
    public synchronized void partitionData(Set<String> pools,
                                           Set<String> pnfsids,
                                           ReplicationOperationMode mode) {
        for (PnfsIdMetadata opData: registry.values()) {
            if (opData.getMode().equals(mode)) {
                pnfsids.add(opData.pnfsId.toString());
                pools.add(opData.poolName);
            }
        }
    }

    public synchronized String print() {
        StringBuilder replicate = new StringBuilder();
        StringBuilder reduce = new StringBuilder();

        replicate.append("REPLICATE OPERATIONS:\n\n");
        reduce.append("REDUCE OPERATIONS:\n\n");

        int repCount = 0;
        int redCount = 0;

        for (PnfsIdMetadata opData: registry.values()) {
            switch(opData.getMode()) {
                case REPLICATE:
                    replicate.append("[")
                             .append(++repCount)
                             .append("]\t")
                             .append(getKey(opData))
                             .append("\trequested ")
                             .append(opData.getAbsoluteDelta())
                             .append("\tstarting count ")
                             .append(opData.getOriginalCount())
                             .append("\tcurrent count ")
                             .append(opData.getReplicaPools().size())
                             .append("\tacks ")
                             .append(opData.successAcknowledgments)
                             .append("\n");
                    break;
                case REDUCE:
                    reduce.append("[")
                          .append(++redCount)
                          .append("]\t")
                          .append(getKey(opData))
                          .append("\trequested ")
                          .append(opData.getAbsoluteDelta())
                          .append("\tstarting count ")
                          .append(opData.getOriginalCount())
                          .append("\tcurrent count ")
                          .append(opData.getReplicaPools().size())
                          .append("\n");
                    break;
                case NONE:
                    break;
            }
        }

        replicate.append(SEPARATOR).append("\n");
        reduce.append(SEPARATOR);

        String totals = "IN PROGRESS:\treplicate "
                        + repCount + "\treduce " + redCount
                        + SEPARATOR + "\n";

        return totals + replicate.toString() + reduce.toString();
    }

    public synchronized void register(PnfsIdMetadata opData) {
        String key = getKey(opData);
        if (key != null) {
            registry.put(key, opData);
        }
    }

    public synchronized void remove(String key) {
        registry.remove(key);
    }

    public synchronized void removeMatchingKeys(String expression) {
        Pattern pattern = Pattern.compile(expression);
        for (Iterator<PnfsIdMetadata> it
                        = registry.values().iterator(); it.hasNext();) {
            PnfsIdMetadata opData = it.next();
            String key = getKey(opData);
            if (pattern.matcher(key).find()) {
                it.remove();
            }
        }
    }

    public void setUtils(ReplicationQueryUtilities utils) {
        this.utils = utils;
    }

    public synchronized void unregister(PnfsIdMetadata opData) {
        String key = getKey(opData);
        if (key != null) {
            registry.remove(key);
        }
    }

    public synchronized PnfsIdMetadata update(PnfsIdMetadata opData,
                                              PnfsModifyCacheLocationMessage message)
                    throws CacheException, InterruptedException {
        String key = getKey(opData);
        opData = registry.get(key);

        String pool = message.getPoolName();

        LOGGER.debug("update {}, {}", key, pool);

        switch(opData.sourceType) {
            case CLEAR:
                opData.removeReplicaPool(pool);

                switch(opData.getMode()) {
                    case REDUCE:
                        if (opData.isRequestSatisfied()) {
                            registry.remove(key);
                        }
                        break;
                    case REPLICATE:
                        /*
                         * A corrupted file was picked during a replication
                         * operation, whose subsequent removal triggered this
                         * clear cache location message.  We need to choose
                         * another source, copy the current metadata with
                         * the new source pool, and return it so a new
                         * task can be submitted.
                         */
                        String source = utils.removeRandomEntry(opData.getReplicaPools());

                        if (source == null) {
                            registry.remove(key);
                            LOGGER.error(AlarmMarkerFactory.getMarker(Severity.HIGH,
                                                                      PnfsIdMetadata.ALARM_INACCESSIBLE,
                                                                      opData.pnfsId.toString()),
                                            "{} is corrupted an alternate source"
                                            + " pool cannot be located; "
                                            + "abandoning operation.",
                                            opData.pnfsId);
                            return opData;
                        }

                        /*
                         * add back source pool, since it contains replica
                         */
                        opData.addReplicaPool(source);

                        /*
                         * clone the data
                         */
                        opData = opData.copy(source);

                        /*
                         * replace entry (the key is the same)
                         */
                        registry.put(key, opData);

                        break;
                    case NONE:
                        throw new IllegalStateException("trying to update operation "
                                        + opData
                                        + " with undefined mode; this should not "
                                        + "happen and is likely a bug");
                }
                break;
            case ADD:
                if (!opData.hasReplica(pool)) {
                    opData.addReplicaPool(pool);
                }

                if (opData.isRequestSatisfied()) {
                    /*
                     * we can pre-empt the ACK counting in this case.
                     */
                    registry.remove(key);
                }
                break;
            default:
                /*
                 * should not happen (SCAN messages are blocked from update)
                 */
        }
        return opData;
    }

    public synchronized PnfsIdMetadata update(ReplicationStatusMessage message) {
        String key = getOperationTokenKey(message.pnfsid, message.poolGroup);
        PnfsIdMetadata opData = registry.get(key);

        LOGGER.debug("update {}, {}, STATE {}", key, opData, message.jobState);

        if (opData == null) {
            /*
             * operation already removed from map because it
             * received the necessary number of cache location messages
             */
            return null;
        }

        switch(message.jobState) {
            case IN_PROGRESS:
                break;
            default:
                opData.successAcknowledgments.incrementAndGet();
        }

        int acks = opData.successAcknowledgments.get();
        int needed = opData.getAbsoluteDelta();
        boolean shouldRemove = acks >= needed;

        switch(message.jobState) {
            case COPY_SUCCEEDED:
            case COPY_EXISTS:
                /*
                 * We need to make sure all the expected messages arrived.
                 * XXX Eventually when we can distinguish between these
                 * two cases, COPY_EXISTS should add a placeholder
                 * to the replica list before checking if request is satisfied.
                 */
                shouldRemove &= opData.isRequestSatisfied();
                break;
            default:
        }

        if (shouldRemove) {
            registry.remove(key);
        }

        return opData;
    }

    private String getKey(PnfsIdMetadata opData) {
        return getOperationTokenKey(opData.pnfsId,
                                    opData.poolGroupData.poolGroup);
    }
}
