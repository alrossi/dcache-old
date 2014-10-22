package org.dcache.replication.v3.namespace.handlers.message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import dmg.cells.nucleus.CDC;

import org.dcache.replication.v3.namespace.ResilienceWatchdog;

/**
 * @author arossi
 *
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

    /**
     * For package use by all message handlers.
     */
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
