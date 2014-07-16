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

import diskCacheV111.vehicles.PnfsModifyCacheLocationMessage;
import diskCacheV111.vehicles.PoolStatusChangedMessage;

import dmg.cells.nucleus.CellMessageReceiver;

import org.dcache.vehicles.replication.CorruptFileMessage;
import org.dcache.vehicles.replication.ReplicationStatusMessage;

/**
 * Makes explicit the actual messages to be handled by the receiver.
 *
 * @author arossi
 */
public interface ReplicationMessageReceiver extends CellMessageReceiver {
    final String ADD_CACHE_LOCATION_MSGS = "ADD CACHE LOCATION";
    final String CLEAR_CACHE_LOCATION_MSGS = "CLEAR CACHE LOCATION";
    final String POOL_STATUS_CHANGED_MSGS = "POOL STATUS CHANGED";
    final String COPY_JOB_STATUS_MSGS = "PNFSID COPY JOB STATUS";
    final String CORRUPT_FILE_MSGS = "CORRUPT FILE";
    final String MESSAGES = "MESSAGES";

    /**
     * Provision is made by this message for the elimination of a file found to
     * have incorrect size or checksum data, either when attempting to replicate
     * or during pool scanning/scrubbing.
     */
    void messageArrived(CorruptFileMessage message);

    /**
     * Should determine whether the pnfsid+pool data correspond to a a currently
     * running replication request. Should determine from the pool and the file
     * storage info whether replicas are required, and initiate a request for
     * either replication or reduction.
     */
    void messageArrived(PnfsModifyCacheLocationMessage message);

    /**
     * Should check the status of the pool, and act appropriately; usually this
     * would mean issuing a check on the pool's files for deficient copies (if
     * the pool goes off-line) or for redundant copies (if it comes on-line),
     * and sending any necessary requests.
     */
    void messageArrived(PoolStatusChangedMessage message);

    /**
     * Provision is made by this message for the return of request status, in
     * order to determine whether an operation can be removed from the
     * {@link ReplicationOperationRegistry}.
     */
    void messageArrived(ReplicationStatusMessage message);

    /**
     * Allows a controller unit to turn on message processing (for internal use).
     */
    void enableMessages();

    /**
     * Allows a controller unit to turn off message processing (for internal use).
     */
    void disableMessages();
}
