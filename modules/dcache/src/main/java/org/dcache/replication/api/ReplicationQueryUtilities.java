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

import java.util.List;
import java.util.concurrent.TimeoutException;

import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPoolGroup;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;

import dmg.cells.nucleus.CellEndpoint;

import org.dcache.replication.data.PoolMetadata;
import org.dcache.vehicles.replication.ListPnfsidsForPoolMessage;

/**
 * Useful shared methods for querying cache locations.
 *
 * @author arossi
 */
public interface ReplicationQueryUtilities {

    /**
     * @return a list of the names of the pools belonging to the given
     *         pool group and which are currently not disabled.
     */
    List<String> getActivePoolsInGroup(SelectionPoolGroup poolGroup,
                                       PoolSelectionUnit psu);

    /**
     * @return a list of the names of the pools containing replicas of
     *         the given file.
     */
    List<String> getCacheLocations(PnfsId pnfsId) throws CacheException,
                    InterruptedException;

    /**
     * @param  data contains information about the pool's group.
     * @param  type of scan (e.g., MIN, MAX, etc.) to be done.
     * @param  activeOnly if true, exclude pools that are disabled in
     *         counting the replicas.
     * @return a message to be sent to the namespace requesting a list
     *         of (pnfsid, location) pairs for files for which the number
     *         of copies is either deficient or in excess or both.
     */
    ListPnfsidsForPoolMessage getListMessage(PoolMetadata data,
                                             ReplicationScanType type,
                                             boolean activeOnly)
                    throws CacheException, InterruptedException,
                    TimeoutException;

    /**
     * @param  pool to be scanned.
     * @param  type of scan (e.g., MIN, MAX, etc.) to be done.
     * @param  minimum number of replicas.
     * @param  maximum number of replicas.
     * @param  exclude list of pools to exclude from the counting of replcas.
     * @return a message to be sent to the namespace requesting a list
     *         of (pnfsid, location) pairs for files for which the number
     *         of copies is either deficient or in excess or both.
     */
    ListPnfsidsForPoolMessage getListMessage(String pool,
                                             ReplicationScanType type,
                                             int minimum,
                                             int maximum,
                                             List<String> exclude);

    /**
     * Fills the three (non-<code>null</code>) collections with the names of all
     * pools, active pools and inactive pools, respectively.
     *
     * @param pools
     *            can be <code>null</code>.
     * @param activePools
     *            can be <code>null</code>.
     * @param inactivePools
     *            can be <code>null</code>.
     */
    void getPoolsInGroup(SelectionPoolGroup poolGroup,
                         PoolSelectionUnit psu,
                         List<String> pools,
                         List<String> activePools,
                         List<String> inactivePools);

    /**
     * The contract for expressing resilience for a pool is that a pool can
     * belong to only one <i>resilient</i> pool group.
     *
     * @return the first group to have replica minimum > 1; else
     *         <code>null</code>.
     */
    SelectionPoolGroup getResilientPoolGroupOfPool(String pool,
                                                   PoolSelectionUnit psu)
                    throws CacheException, InterruptedException;

    /**
     * @return true if the system sticky bit is on. Note that precious files are
     *         included here, provided they have this sticky bit set as well.
     */
    boolean isSystemSticky(String pnfsid, String pool, CellEndpoint endpoint)
                    throws InterruptedException;

    /**
     * @return true if copy requests should ask for the maximum and
     *         remove requests should reduce the count to the minimum,
     *         false if copy requests should ask for the minimum and
     *         remove requests should reduce the count to the maximum.
     */
    boolean isUseGreedyRequests();

    /**
     * @param list
     *            from which the randomly chosen entry is removed.
     * @return the chosen entry.
     */
    String removeRandomEntry(List<String> list);

    /**
     * Pool group has been determined to be resilient. For each active pool in
     * the group, a full scan task is submitted.
     */
    void scanActivePools(SelectionPoolGroup poolGroup, PoolSelectionUnit psu);
}
