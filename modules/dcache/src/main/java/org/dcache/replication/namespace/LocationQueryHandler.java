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
package org.dcache.replication.namespace;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import diskCacheV111.util.CacheException;

import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.CellMessageSender;
import dmg.util.command.DelayedCommand;

import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.JdbcFs;
import org.dcache.replication.api.ReplicationScanType;
import org.dcache.vehicles.replication.ListPnfsidsForPoolMessage;

/**
 * Runs inside the namespace provider cell (Chimera). Its
 * purpose is to service requests for location information without
 * going through the PnfsManager layer.<br><br>
 *
 * Requests to this handler are for the cache locations associated
 * with files on a given pool in a resilient pool group, where
 * either the minimum is not met, or the maximum has been exceeded, or both.
 * The message contains an optional list of pools to be excluded (usually
 * because they are offline) from this count.  The result returns
 * all the locations of the matching pnfsids, including the reference pool.
 * <br><br>
 *
 * These requests are handled by a separate thread pool from those in
 * the PnfsManager.  The executed task is largely a wrapper around
 * a call to the file system layer.  Note, however, that the method
 * called does not involve the Fs_Inode abstraction, since it targets
 * a location (pool) and not a file or directory.<br><br>
 *
 * @see ListPnfsidsForPoolMessage
 *
 * @author arossi
 */
public final class LocationQueryHandler implements CellMessageReceiver,
                                                   CellMessageSender {
    private static final Logger LOGGER
        = LoggerFactory.getLogger(LocationQueryHandler.class);

    class LocationQueryTask extends DelayedCommand {
        private static final long serialVersionUID = 2294802416427103525L;

        private final ListPnfsidsForPoolMessage listMessage;

        LocationQueryTask(ListPnfsidsForPoolMessage listMessage, Executor executor) {
          super(executor);
          this.listMessage = listMessage;
        }

        @Override
        protected Serializable execute() throws Exception {
            String location = listMessage.location;
            ImmutableList<String> excluded = listMessage.getExcludedLocations();
            ReplicationScanType type = listMessage.type;

            Integer min = null;
            Integer max = null;

            switch(type) {
                case PERIODIC:
                case DOWN:
                case MIN:
                case UNIQUE:
                    min = listMessage.minimum;
                    break;
                case RESTART:
                case MAX:
                    break;
            }

            switch(type) {
                case PERIODIC:
                case RESTART:
                case MAX:
                    max = listMessage.maximum;
                    break;
                case DOWN:
                case MIN:
                case UNIQUE:
                    break;
            }

            try {
                LOGGER.trace("QUERY {}, {}, {} ({},{}).", type,
                                                          location,
                                                          excluded,
                                                          min,
                                                          max);
                Multimap<String, String> replicas
                    = fs.getCacheLocations(location, excluded, min, max);
                listMessage.setReplicas(replicas);
                LOGGER.trace("call() returning {}.", listMessage);
            } catch (ChimeraFsException e) {
                LOGGER.error("Query for {} failed: {}.", listMessage,
                                                         e.getMessage());
                listMessage.setFailed(CacheException.DEFAULT_ERROR_CODE,
                                      e.getMessage());
            }

            return listMessage;
        }

    }

    private ExecutorService requestThreadPool;
    private CellEndpoint endpoint;
    private JdbcFs fs;
    private int concurrentRequests = 1;

    public void initialize() {
        requestThreadPool = Executors.newFixedThreadPool(concurrentRequests);
    }

    public void messageArrived(CellMessage message,
                               ListPnfsidsForPoolMessage listMessage) {
        LOGGER.trace("messageArrived " + listMessage);
        try {
            message.revertDirection();
            new LocationQueryTask(listMessage, requestThreadPool)
                .call()
                .deliver(endpoint, message);
        } catch (RuntimeException t) {
            LOGGER.error("Unexpected error during query processing for {}.",
                            listMessage, t);
        } catch (Exception e) {
            LOGGER.error("Error during query processing for {}: {}",
                            listMessage, e.getMessage());
        }
    }

    public void setCellEndpoint(CellEndpoint endpoint) {
        this.endpoint = endpoint;
    }

    public void setConcurrentRequests(int concurrentRequests) {
        this.concurrentRequests = concurrentRequests;
    }

    public void setFileSystem(JdbcFs fs) {
        this.fs = fs;
    }

    public void shutdown() {
        requestThreadPool.shutdown();
    }
}
