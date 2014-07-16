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
package org.dcache.replication.api;

import java.util.Set;

import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPoolGroup;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PnfsModifyCacheLocationMessage;

import org.dcache.replication.data.PnfsIdMetadata;
import org.dcache.vehicles.replication.ReplicationStatusMessage;

/**
 * Defines interaction with the registry which keeps track of in-flight copy or
 * remove operations/requests.
 *
 * @author arossi
 */
public interface ReplicationOperationRegistry {

    /**
     * Should remove all entries from the registry.
     */
    void clear();

    /**
     * @return representation of the contents of the registry.
     */
    String print();

    /**
     * @return unique key defining an operation or group of operations.
     */
    String getOperationTokenKey(PnfsId pnfsId, SelectionPoolGroup poolGroup);

    /**
     * @return unique key defining an operation or group of operations.
     */
    String getOperationTokenKey(String pnfsId, String poolGroup);

    /**
     * @param opData
     *            defines the operation.
     *
     * @return true if its key is registered.
     */
    boolean isRegistered(PnfsIdMetadata opData);

    /**
     * Extracts from the registry all pools and pnsfids currently involved in
     * operations of the given mode.
     *
     * @param mode
     *            to match (either REPLICATE or REDUCE).
     */
    void partitionData(Set<String> pools,
                       Set<String> pnfsids,
                       ReplicationOperationMode mode);

    /**
     * @param opData
     *            defines the operation.
     */
    void register(PnfsIdMetadata opData);

    /**
     * @param key
     *            uniquely identifying an operation.
     */
    void remove(String key);

    /**
     * Remove all entries whose keys contain this pnfsid.
     */
    void removeEntriesWithPnfsid(String pnfsid);

    /**
     * Remove all entries whose keys contain this poolGroup.
     */
    void removeEntriesWithPoolGroup(String poolGroup);

    /**
     * @param opData
     *            defines the operation.
     */
    void unregister(PnfsIdMetadata opData);

    /**
     * When called, the registry should apply the appropriate logic to the
     * combination of current operation state and the message received,
     * modifying the state if applicable and removing the entry if a final state
     * has been reached.
     *
     * @param opData
     *            contained in the registry.
     * @param message
     *            specifying addition or removal of a cache location.
     * @return the current operation (meta)data (for any further
     *         post-processing).
     */
    PnfsIdMetadata update(PnfsIdMetadata opData,
                          PnfsModifyCacheLocationMessage message)
                    throws CacheException, InterruptedException;

    /**
     * When called, the registry should apply the appropriate logic to the
     * combination of current operation state and the message received,
     * modifying the state if applicable and removing the entry if a final state
     * has been reached.
     *
     * @param message
     *            specifying the status of an asynchronous copy or remove.
     * @return the current operation (meta)data (for any further
     *         post-processing).
     */
    PnfsIdMetadata update(ReplicationStatusMessage message);
}
