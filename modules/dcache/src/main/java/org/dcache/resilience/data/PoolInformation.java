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
package org.dcache.resilience.data;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Date;
import java.util.List;

import diskCacheV111.pools.PoolV2Mode;
import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.PoolManagerGetPoolsByNameMessage;
import diskCacheV111.vehicles.PoolManagerPoolInformation;
import diskCacheV111.vehicles.PoolStatusChangedMessage;
import org.dcache.cells.CellStub;
import org.dcache.util.CacheExceptionFactory;
import org.dcache.util.CellStubFactory;
import org.dcache.util.ExceptionMessage;
import org.dcache.vehicles.resilience.GetPoolModeMessage;
import org.dcache.vehicles.resilience.GetPoolTagsMessage;

/**
 * <p>Encapsulates "live" pool information – pool mode, status, and cost.
 * Also stores pool tags.</p>
 *
 * <p>This object is held by the {@link PoolInfoMap}; status, mode, tags and cost
 * are refreshed lazily, the latter two according to a caching scheme.  Status
 * and mode are also updated when a pool status change message is received.
 * </p>
 *
 * Created by arossi on 9/4/15.
 */
final class PoolInformation {
    private static final Logger LOGGER = LoggerFactory.getLogger(
                    PoolInformation.class);

    final CellStub poolManager;
    final CellStub pool;

    Integer key;
    PoolManagerPoolInformation info;
    PoolStatusForResilience status;
    PoolV2Mode mode;
    ImmutableMap<String, String> tags;
    long timestamp = 0;

    PoolInformation(Integer key, String pool, CellStubFactory factory) {
        this.key = key;
        poolManager = factory.getPoolManager();
        this.pool = factory.getPoolStub(pool);
    }

    public String toString() {
        return String.format("key\t\t%s\ntags\t\t%s\nmode\t\t%s\n"
                                        + "status\t\t%s\ninfo\t\t%s\n"
                                        + "last update\t%s\n", key, tags, mode,
                        status, info, new Date(timestamp));
    }

    synchronized boolean canRead() {
        if (status == null) {
            LOGGER.trace("getPoolInformation fetching pool status {}", this);
            refreshStatus();
        }

        return status != PoolStatusForResilience.DOWN
                        && mode.getMode() != PoolV2Mode.DISABLED
                        && !mode.isDisabled(PoolV2Mode.DISABLED_FETCH)
                        && !mode.isDisabled(PoolV2Mode.DISABLED_DEAD);
    }

    synchronized boolean canWrite() {
        if (status == null) {
            LOGGER.trace("getPoolInformation fetching pool status {}", this);
            refreshStatus();
        }

        return status != PoolStatusForResilience.DOWN
                        && mode.getMode() != PoolV2Mode.DISABLED
                        && !mode.isDisabled(PoolV2Mode.DISABLED_STORE)
                        && !mode.isDisabled(PoolV2Mode.DISABLED_DEAD)
                        && !mode.isDisabled(PoolV2Mode.DISABLED_P2P_SERVER);
    }

    /**
     * Allow pool cost to be refreshed lazily.
     */
    synchronized PoolManagerPoolInformation getInfo(List<String> pools, long expiry)
                    throws CacheException, InterruptedException {
        if (isExpired(expiry)) {
            refreshPoolManagerInfo(pools);
        }
        return info;
    }

    /**
     * <p>Pool tags can change on reboot, so check them if this is
     *    a RESTART update.</p>
     * @param update
     */
    synchronized void updateState(PoolStateUpdate update) {
        status = update.status;
        mode = update.mode;
        if (status == PoolStatusForResilience.RESTART) {
            refreshTags();
        }
    }

    private boolean isExpired(long expiry) {
        return System.currentTimeMillis() - timestamp >= expiry;
    }

    private void refreshPoolManagerInfo(List<String> pools)
                    throws CacheException, InterruptedException {
        String pool = pools.get(key);
        LOGGER.trace("get PoolManagerPoolInfo for {}", pool);
        PoolManagerGetPoolsByNameMessage msg
                        = new PoolManagerGetPoolsByNameMessage(ImmutableList.of(pool));
        LOGGER.trace("sending {}", msg);
        msg = poolManager.sendAndWait(msg);
        LOGGER.trace("received {}", msg);
        if (msg != null) {
            Collection<PoolManagerPoolInformation> infos = msg.getPools();
            if (!infos.isEmpty()) {
                info = msg.getPools().iterator().next();
                timestamp = System.currentTimeMillis();
            }
        }
    }

    private void refreshStatus() {
        LOGGER.trace("get pool status for {}", key);
        GetPoolModeMessage msg = new GetPoolModeMessage();
        try {
            msg = this.pool.sendAndWait(msg);
            if (msg == null) {
                throw CacheExceptionFactory.exceptionOf(
                                CacheException.SERVICE_UNAVAILABLE,
                                "Could not fetch pool mode.");
            }
            mode = msg.getMode();
        } catch (CacheException e) {
            LOGGER.debug("Could not fetch pool mode from pool: {}",
                         new ExceptionMessage(e));
            // consider it down
            mode = new PoolV2Mode(PoolV2Mode.DISABLED);
            status = null;
        } catch (InterruptedException e) {
            LOGGER.trace("Fetch of pool mode from pool interrupted.");
            return;
        }

        LOGGER.trace("Fetch of pool mode from pool manager {}.", mode);
        status = PoolStatusForResilience.getStatusFor(PoolStatusChangedMessage.UP,
                                                      mode);
    }

    private void refreshTags() {
        LOGGER.trace("get pool tags for {}", key);
        GetPoolTagsMessage msg = new GetPoolTagsMessage();
        try {
            msg = this.pool.sendAndWait(msg);
            if (msg == null) {
                throw CacheExceptionFactory.exceptionOf(
                                CacheException.SERVICE_UNAVAILABLE,
                                "Could not fetch pool tags.");
            }
            tags = msg.getTags();
        } catch (CacheException e) {
            LOGGER.debug("Could not fetch pool tags from pool: {}.",
                            new ExceptionMessage(e));
        } catch (InterruptedException e) {
            LOGGER.trace("Fetch of pool tags from pool interrupted.");
        }
    }
}
