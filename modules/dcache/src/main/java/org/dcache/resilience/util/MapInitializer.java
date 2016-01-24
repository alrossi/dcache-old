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
package org.dcache.resilience.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.PoolManagerGetPoolMonitor;
import org.dcache.alarms.AlarmMarkerFactory;
import org.dcache.alarms.PredefinedAlarm;
import org.dcache.cells.CellStub;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.resilience.data.PnfsOperationMap;
import org.dcache.resilience.data.PoolInfoMap;
import org.dcache.resilience.data.PoolOperationMap;
import org.dcache.resilience.handlers.PoolOperationHandler;
import org.dcache.resilience.handlers.ResilienceMessageHandler;
import org.dcache.util.ExceptionMessage;

/**
 * <p>Initialization mechanism for resilience maps.
 *
 * <p>Initialize loads the maps in order. Makes sure all incomplete
 *    operations are reloaded from the checkpoint file before starting
 *    the operations map.</p>
 *
 * <p>Receives a relay of reload messages from the pool selection unit
 *    (through the message handler) and processes them, rescanning pools
 *    which are either new or for which the configuration has changed.</p>
 *
 * Created by arossi on 9/21/15.
 */
public final class MapInitializer implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger( MapInitializer.class);
    private static final String INIT_ALARM
                    = "Could not fetch pool selection unit; "
                    + "resilience could not start.";

    private CellStub                 poolManager;
    private PoolInfoMap              poolInfoMap;
    private PnfsOperationMap         pnfsOperationMap;
    private PoolOperationMap         poolOperationMap;
    private PoolOperationHandler     poolOperationHandler;
    private ResilienceMessageHandler messageHandler;
    private MessageGuard             messageGuard;
    private PoolSelectionUnit        psu;
    private Long                     initialized;
    private String                   initError;

    public synchronized String getInitError() {
        return initError;
    }

    public boolean initialize() {
        if (isInitialized()) {
            return false;
        }

        new Thread(this, "map-initializer").start();
        return true;
    }

    public synchronized boolean isInitialized() {
        return initialized != null;
    }

    /**
     *  <p>Reload messages can be triggered either by a restart of
     *       the PoolManager or by a reload command.</p>
     *
     *  <p>In the former case, there is the possibility of added turbulence
     *       if the PoolManager is starting in conjunction with the rest of
     *       the head nodes (i.e., where resilience runs).</p>
     *
     *  <p>In an attempt to avoid producing scan failures during startup
     *       (since pools may not yet be available and locations will
     *       be thought of as offline), this message is dropped if it arrives
     *       within 1 minute of initialization.</p>
     */
    public void reloadPsu(PoolSelectionUnit psu) {
        synchronized (this) {
            if (initialized == null ||
                        (System.currentTimeMillis() - initialized <
                                        TimeUnit.MINUTES.toMillis(1))) {
                return;
            }
            this.psu = psu;
        }

        poolOperationHandler.getSubmitService().submit(this::reloadAndScan);
    }

    public void run() {
        if (isInitialized()) {
            return;
        }

        LOGGER.info("Waiting for pool status message frequency to drop to 0/sec.");
        messageGuard.waitForPoolStatusThreshold();

        LOGGER.info("Fetching pool selection unit.");

        getPsu();

        if (psu == null) {
            initError = INIT_ALARM;
            LOGGER.error(AlarmMarkerFactory.getMarker(PredefinedAlarm.RESILIENCE_OFFLINE,
                            "resilience"), INIT_ALARM);
            return;
        }

        /*
         *  Synchronous, order must be maintained.
         */
        LOGGER.info("Loading psu information.");
        poolInfoMap.reload(psu);
        LOGGER.info("Loading pool information.");
        poolOperationMap.loadPools();
        LOGGER.info("Maps reloaded; starting consumer threads.");
        poolOperationMap.initialize();
        LOGGER.info("Maps reloaded; delivering backlog.");
        messageGuard.activateAndDeliverBacklog();
        pnfsOperationMap.initialize();
        LOGGER.info("Reloading checkpoint file.");
        pnfsOperationMap.reload();
        LOGGER.info("Map initialization complete.");

        synchronized (this) {
            initialized = System.currentTimeMillis();
        }
    }

    public void setMessageGuard(MessageGuard messageGuard) {
        this.messageGuard = messageGuard;
    }

    public void setMessageHandler(
                    ResilienceMessageHandler messageHandler) {
        this.messageHandler = messageHandler;
    }

    public void setPnfsOperationMap(PnfsOperationMap pnfsOperationMap) {
        this.pnfsOperationMap = pnfsOperationMap;
    }

    public void setPoolInfoMap(PoolInfoMap poolInfoMap) {
        this.poolInfoMap = poolInfoMap;
    }

    public void setPoolManager(CellStub poolManager) {
        this.poolManager = poolManager;
    }

    public void setPoolOperationHandler(
                    PoolOperationHandler poolOperationHandler) {
        this.poolOperationHandler = poolOperationHandler;
    }

    public void setPoolOperationMap(
                    PoolOperationMap poolOperationMap) {
        this.poolOperationMap = poolOperationMap;
    }

    /**
     * <p>Wait for an acknowledgment.  Should automatically retry on
     *    no route to cell.</p>
     */
    private void getPsu() {
        PoolManagerGetPoolMonitor msg = new PoolManagerGetPoolMonitor();
        msg.setReplyRequired(true);

        PoolMonitor monitor = null;

        /*
         * During system startup it is sometimes necessary to retry after
         * the initial timeout.  This needs to be better understood.
         */
        for (int i = 0; i < 3; i++) {
            try {
                msg = poolManager.sendAndWait(msg);
            } catch (InterruptedException | CacheException e) {
                LOGGER.error("Could not retrieve pool monitor: {}",
                             new ExceptionMessage(e));
            }

            if (msg != null) {
                monitor = msg.getPoolMonitor();
                if (monitor != null) {
                    break;
                }
            }
        }

        if (monitor == null) {
            LOGGER.error("Could not retrieve pool monitor:  "
                                         + "returned message had no monitor.");
            return;
        }

        LOGGER.trace("Received message {}", msg);
        psu = msg.getPoolMonitor().getPoolSelectionUnit();
    }

    private void reloadAndScan() {
        LOGGER.trace("Reloading psu information");
        Collection<Integer> poolsToRescan = poolInfoMap.reload(psu);
        poolOperationMap.loadPools();
        LOGGER.trace("pools to rescan: {}", poolsToRescan);
        poolsToRescan.stream()
                     .map((i) -> poolInfoMap.getPool(i))
                     .forEach(messageHandler::scanPool);
    }
}
