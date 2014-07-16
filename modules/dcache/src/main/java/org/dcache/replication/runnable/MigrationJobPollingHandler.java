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
package org.dcache.replication.runnable;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPoolGroup;
import diskCacheV111.util.CacheException;

import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.SerializationException;

import org.dcache.cells.CellStub;
import org.dcache.replication.api.ReplicationEndpoints;
import org.dcache.replication.api.ReplicationMessageReceiver;
import org.dcache.replication.api.ReplicationOperationMode;
import org.dcache.replication.api.ReplicationOperationRegistry;
import org.dcache.replication.api.ReplicationQueryUtilities;
import org.dcache.replication.messages.SynchronizedMessageReceiver;
import org.dcache.vehicles.replication.ReplicationStatusMessage;
import org.dcache.vehicles.replication.ReplicationStatusMessage.State;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Provisional module which polls all pools for outstanding migration jobs, and
 * calls {@link SynchronizedMessageReceiver#messageArrived(ReplicationStatusMessage)}.
 * This should either be replaced by a callback from the
 * migration module itself, or, absent that, the API for the migration module
 * should handle this message request directly rather than requiring
 * the client to parse string output.
 *
 * @author arossi
 */
public final class MigrationJobPollingHandler extends RunnableModule {
    private static final String MIGRATION_LS = "migration ls";
    private static final String MIGRATION_CLEAR = "migration clear";
    private static final int CLEAR_PERIOD = 3; // clear every 3 passes
    private static final Pattern STATE_PATTERN
        = Pattern.compile("\\[([\\d]+)\\][\\s]+([A-Z]+)[\\s]+.+[\\s][-]"
                        + "pnfsid=([0-9A-Z]+)[\\s]+.+");

    private ReplicationEndpoints hub;
    private ReplicationMessageReceiver handler;
    private ReplicationOperationRegistry map;
    private ReplicationQueryUtilities utils;

    private long counter = 0;

    public void initialize() {
        checkNotNull(hub);
        checkNotNull(handler);
        checkNotNull(map);
        checkNotNull(utils);
        super.initialize();
    }

    @Override
    public void run() {
        long waitPeriod = timeoutUnit.toMillis(timeout);
        while (!Thread.interrupted()) {
            try {
                Thread.sleep(waitPeriod);
            } catch (InterruptedException ie) {
                break;
            }

            CellEndpoint endpoint = hub.getEndpoint();

            /*
             * Bin the pools and pnfsids.
             */
            Set<String> pools = new HashSet<>();
            Set<String> pnfsids = new HashSet<>();

            map.partitionData(pools, pnfsids, ReplicationOperationMode.REPLICATE);

            /*
             * Generate relevant messages.
             */
            Set<ReplicationStatusMessage> messages = new HashSet<>();
            for (String pool: pools) {
                String reply
                    = requestMigrationStatusFromPool(pool, endpoint);
                if (reply != null) {
                    handleMigrationLs(pool, pnfsids, messages, reply);
                }

                if (counter % CLEAR_PERIOD == 0) {
                    issueClearMigration(pool, endpoint);
                }
            }

            for (ReplicationStatusMessage message: messages) {
                handler.messageArrived(message);
            }
        }

        LOGGER.debug("Thread interrupted, exiting ...");
    }

    public void setHub(ReplicationEndpoints hub) {
        this.hub = hub;
    }

    public void setHandler(ReplicationMessageReceiver handler) {
        this.handler = handler;
    }

    public void setMap(ReplicationOperationRegistry map) {
        this.map = map;
    }

    public void setUtils(ReplicationQueryUtilities utils) {
        this.utils = utils;
    }

    /*
     * Parses the polling output from the pool and sends message
     * to the callback.
     */
    private void handleMigrationLs(String pool,
                                   Set<String> pnfsids,
                                   Set<ReplicationStatusMessage> messages,
                                   String reply) {
        SelectionPoolGroup pgroup;
        try {
            pgroup
                = utils.getResilientPoolGroupOfPool(pool,
                                                    hub.getPoolMonitor()
                                                       .getPoolSelectionUnit());
        } catch (CacheException | InterruptedException t1) {
            LOGGER.error("could not get pool group for {}; this is a bug! "
                            + "please check log trace", pool);
            LOGGER.trace("bug in call to get replicating pool group", t1);
            return;
        }

        BufferedReader reader = new BufferedReader(new StringReader(reply));
        long count = 0;

        while (true) {
            try {
                String line = reader.readLine();
                if (line == null) {
                    break;
                }
                ++count;

                LOGGER.debug("{}:  {}", count, line);

                Matcher m = STATE_PATTERN.matcher(line);
                if (!m.find()) {
                    LOGGER.debug("Badly formed list line: '{}'.", line);
                    continue;
                }

                String pnfsid = m.group(3);
                if (!pnfsids.contains(pnfsid)) {
                    LOGGER.debug("{} is not being tracked.", pnfsid);
                    continue;
                }

                State state = State.convert(m.group(2));
                messages.add(new ReplicationStatusMessage(pool,
                                                          pnfsid,
                                                          pgroup,
                                                          state));
            } catch (IOException t) {
                LOGGER.error("Error while processing reply {} from pool {}: {}.",
                                reply,
                                pool,
                                t.getMessage());
                break;
            }
        }

        try {
            reader.close();
        } catch (IOException t) {
            LOGGER.error("Problem closing reader: {}.", t.getMessage());
        }

        LOGGER.debug("Reply from pool {} contained {} job ids; "
                        + "matches with current messages {}.",
                        pool,
                        count,
                        messages.size());
    }

    /*
     * Sends request to the pool to list all migration jobs.
     */
    private String requestMigrationStatusFromPool(String pool,
                                                  CellEndpoint endpoint) {
        String message = null;
        CellStub stub = new CellStub();
        stub.setCellEndpoint(endpoint);
        stub.setDestination(pool);
        stub.setTimeout(timeout);
        stub.setTimeoutUnit(timeoutUnit);

        try {
            message = stub.sendAndWait(MIGRATION_LS, String.class);
        } catch (SerializationException | InterruptedException | CacheException t) {
            LOGGER.error("Problem with migration info request to {}.", pool, t);
        }

        if (message == null) {
            LOGGER.debug("Migration info request to {} timed out.", pool);
        }

        return message;
    }

    /*
     * Implementation specific.  This makes sure finalized jobs are
     * cleared, to minimize message size.
     */
    private void issueClearMigration(String pool, CellEndpoint endpoint) {
        CellStub stub = new CellStub();
        stub.setCellEndpoint(endpoint);
        stub.setDestination(pool);
        stub.setTimeout(timeout);
        stub.setTimeoutUnit(timeoutUnit);

        try {
            stub.send(MIGRATION_CLEAR, String.class);
        } catch (SerializationException t) {
            LOGGER.error("Problem with migration info request to {}.", pool, t);
        }
    }
}
