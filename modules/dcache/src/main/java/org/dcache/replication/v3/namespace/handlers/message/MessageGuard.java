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
package org.dcache.replication.v3.namespace.handlers.message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import dmg.cells.nucleus.CDC;

import org.dcache.replication.v3.namespace.ResilienceWatchdog;

/**
 * Used to ensure that the handlers are active
 * and that the incoming message does not carry the replica manager session id.
 * This is to avoid initiating unnecessary replica requests on copies
 * just made by a request originating in the replica manager itself.
 * While such redundant requests will not cause an infinite
 * cycle, as the migration task in which this bottoms out will not
 * continue indefinitely creating copies (and thus triggering update
 * messages to the namespace which are intercepted by the replica manager),
 * they would generate more work than is necessary or desirable.
 *
 * @author arossi
 */
public class MessageGuard {
    /**
     * One-time alarm clock thread. The message handler can be paused for an
     * initial period before beginning to process intercepted messages. This is
     * useful because a cold start of an entire system generates multiple pool
     * state messages which usually do not need to be processed.
     */
    private class AlarmClock extends Thread {
        @Override
        public void run() {
            if (initialWait > 0) {
                long waitInMs = initialWaitUnit.toMillis(initialWait);

                try {
                    Thread.sleep(waitInMs);
                } catch (InterruptedException ie) {
                    LOGGER.debug("Thread interrupted during initial wait.");
                }
            }

            accept.set(true);

            if (watchdog != null) {
                watchdog.initialize();
            }
        }
    }

    private static final Logger LOGGER
        = LoggerFactory.getLogger(MessageGuard.class);
    private final String replicaId = "REPLICAMANAGER" + UUID.randomUUID();

    private final AtomicBoolean accept = new AtomicBoolean(false);

    private long initialWait = 1;
    private TimeUnit initialWaitUnit = TimeUnit.MINUTES;
    private ResilienceWatchdog watchdog;

    public void initialize() {
        new AlarmClock().start();
    }

    public void setInitialWait(long initialWait) {
        this.initialWait = initialWait;
    }

    public void setInitialWaitUnit(TimeUnit initialWaitUnit) {
        this.initialWaitUnit = initialWaitUnit;
    }

    public void setWatchdog(ResilienceWatchdog watchdog) {
        this.watchdog = watchdog;
    }

    boolean acceptMessage(String message, Object messageObject) {
        LOGGER.trace("**** acceptMessage **** {}: {}.", message, messageObject);

        if (!accept.get()) {
            LOGGER.trace("Replica Manager message handler is paused, "
                            + "message {} will be dropped.", message);
            return false;
        }

        /*
         * A check of the session id ensures that we avoid cyclical calls to
         * replicate the same pnfsid by processing the setAttribute calls made
         * for each of the copies requested by the replica manager. Only
         * operations originating here will carry this id, so messages from
         * other copy operations (e.g. normal p2p) will be handled and not
         * discarded.
         */
        if (CDC.getSession().equals(replicaId)) {
            LOGGER.debug("{} originated with this replica manager ({}); "
                            + "discarding.", message, replicaId);
            return false;
        }

        CDC.setSession(replicaId);

        return true;
    }
}
