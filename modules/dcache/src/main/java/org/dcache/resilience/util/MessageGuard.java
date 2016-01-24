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

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import diskCacheV111.vehicles.Message;
import dmg.cells.nucleus.CDC;

/**
 * <p>Used to check whether the incoming message carries the replication
 * handler session id. This is necessary to distinguish messages
 * which require handling from those which do not.</p>
 *
 * <p>Also stores messages which arrive during the WAITING state, and
 * provides them on a one-time-basis to the caller, presumably during startup.</p>
 *
 * <p>Class is not marked final for stubbing/mocking purposes.</p>
 *
 * Created by arossi on 1/25/15.
 */
public class MessageGuard {
    private static final String RESILIENCE_KEY = "RESILIENCE-";

    @VisibleForTesting
    static final String RESILIENCE_ID  = RESILIENCE_KEY + UUID.randomUUID();

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageGuard.class);

    public static void setResilienceSession() {
        CDC.setSession(RESILIENCE_ID);
    }

    public enum Status {
        INACTIVE,        // replication handler not yet running
        REPLICA,         // message contains the replication session id
        EXTERNAL         // message does not contain replication session id
    }

    class PoolStatusMessageMeter {
        long lastCheck      = System.currentTimeMillis();
        long eventsReceived = 0;

        double getFrequency() {
            long elapsed = System.currentTimeMillis() - lastCheck;
            double secs = TimeUnit.MILLISECONDS.toSeconds(elapsed);

            if (secs == 0.0) {
                lastCheck = System.currentTimeMillis();
                eventsReceived = 0;
                return Double.MAX_VALUE;
            }

            double frequency = eventsReceived / secs;
            LOGGER.debug("getFrequency, frequency is {}/sec.",
                            frequency);

            eventsReceived = 0;
            lastCheck = System.currentTimeMillis();
            return frequency;
        }
    }

    private final LinkedList<Message>    backlog = new LinkedList<>();
    private final PoolStatusMessageMeter meter   = new PoolStatusMessageMeter();

    private BackloggedMessageHandler backlogHandler;
    private boolean                  active;
    private boolean                  enabled;

    private long sampleInterval = 0;
    private TimeUnit sampleIntervalUnit = TimeUnit.SECONDS;

    public void activateAndDeliverBacklog() {
        synchronized (backlog) {
            if (!enabled) {
                return;
            }

            active = true;

            backlog.stream().forEach(
                            (m) -> backlogHandler.processBackloggedMessage(m));
            backlog.clear();
        }
    }

    /**
     * @param message       informative statement for logging purposes.
     * @param messageObject received by the handler.
     * @return status of the message (WAITING, REPLICA, EXTERNAL).
     */
    public Status getStatus(String message, Object messageObject) {
        LOGGER.trace("**** acceptMessage **** {}: {} -- {}/{}.", message,
                     messageObject, enabled, active);

        synchronized (backlog) {
            if (!enabled) {
                LOGGER.trace("{}: {}.", message, Status.INACTIVE);
                return Status.INACTIVE;
            }

            if (!active) {
               /*
                * Hold on to the message so that it can be delivered later.
                * Under normal circumstances, this inactive state should be
                * ephemeral.
                */
                if (messageObject instanceof Message) {
                    backlog.add((Message) messageObject);
                }
                LOGGER.trace("{}: {}.", message, Status.INACTIVE);
                return Status.INACTIVE;
            }

            String session = CDC.getSession();
            LOGGER.trace("{} – session {}", message, session);
            if (session != null && session.startsWith(RESILIENCE_KEY)) {
                LOGGER.trace("{} originated within the replication system ({}).",
                             message, RESILIENCE_ID);
                return Status.REPLICA;
            }

            LOGGER.trace("{}: {}.", message, Status.EXTERNAL);
            return Status.EXTERNAL;
        }
    }

    public void incrementPoolStatusMessageCount() {
        synchronized(meter) {
            ++meter.eventsReceived;
        }
    }

    public void initialize() {
        synchronized (backlog) {
            active = false;
            enabled = true;
        }
    }

    public void setBacklogHandler(BackloggedMessageHandler backlogHandler) {
        this.backlogHandler = backlogHandler;
    }

    public void setEnabled(boolean enabled) {
        synchronized (backlog) {
            this.enabled = enabled;
        }
    }

    public void setSampleInterval(long sampleInterval) {
        this.sampleInterval = sampleInterval;
    }

    public void setSampleIntervalUnit(TimeUnit sampleIntervalUnit) {
        this.sampleIntervalUnit = sampleIntervalUnit;
    }

    public void shutDown() {
        synchronized (backlog) {
            active = false;
            enabled = false;
        }
    }

    public void waitForPoolStatusThreshold() {
        long interval = sampleIntervalUnit.toMillis(sampleInterval);
        if (interval == 0) {
            return;
        }

        synchronized (meter) {
            while (meter.getFrequency() > 0.0) {
                try {
                    meter.wait(interval);
                } catch (InterruptedException e) {
                    LOGGER.trace("waitForPoolStatusThreshold interrupted.");
                    return;
                }
            }
        }
    }
}
