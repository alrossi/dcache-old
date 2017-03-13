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
package org.dcache.restful.services.admin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import dmg.cells.nucleus.CellCommandListener;
import dmg.util.command.Option;
import org.dcache.restful.util.admin.CellMessagingCollector;
import org.dcache.util.RunnableModule;

/**
 * <p>Services supporting restful admin resources should extend
 *    this class and implement their specific service interface.</p>
 *
 * <p>Here is provided a common framework for initializing, resetting
 *    and shutting down the thread which calls the collector(s) and
 *    builds the cache(s).  This includes two admin shell commands
 *    for checking current status and for resetting the timeout.</p>
 */
public abstract class CellDataCachingService<C extends CellMessagingCollector>
                extends RunnableModule
                implements TimestampedService, CellCommandListener {
    protected static final Logger LOGGER
                    = LoggerFactory.getLogger(CellDataCachingService.class);

    /**
     * <p>This class should be subclassed in order to provide the
     * correct command annotation and to distinguish is from
     * other similar commands in the same domain.</p>
     */
    protected abstract class InfoCommand implements Callable<String> {
        @Override
        public String call() {
            StringBuilder builder = new StringBuilder();
            builder.append("    Update Interval : ")
                   .append(timeoutUnit.toSeconds(timeout))
                   .append(" seconds")
                   .append("\n")
                   .append("    Counter : ")
                   .append(processCounter)
                   .append("\n")
                   .append("    Last Time Used : ")
                   .append(timeUsed)
                   .append(" milliseconds")
                   .append("\n");
            return builder.toString();
        }
    }

    /**
     * <p>This class should be subclassed in order to provide the
     * correct command annotation and to distinguish is from
     * other similar commands in the same domain.</p>
     */
    protected abstract class UpdateCommand implements Callable<String> {
        @Option(name = "timeout",
                        usage = "Length of timeout interval; must be >= 10 seconds.")
        Long timeout;

        @Option(name = "unit",
                        valueSpec = "SECONDS|MINUTES|HOURS",
                        usage = "Timeout interval unit (default is SECONDS).")
        TimeUnit unit = TimeUnit.SECONDS;

        @Option(name = "run",
                        usage = "Run update now.")
        boolean run = false;

        @Override
        public String call() {
            String response = "";

            synchronized (CellDataCachingService.this) {
                if (timeout != null) {
                    if (unit.toSeconds(timeout) < 10L) {
                        throw new IllegalArgumentException(
                                        "Update time must exceed 10 seconds");
                    }

                    CellDataCachingService.this.timeout = timeout;
                    CellDataCachingService.this.timeoutUnit = unit;
                    /*
                     * Time the collector communication out at half the
                     * refresh interval.
                     */
                    collector.reset(timeout/2, timeoutUnit);
                    rebuildCaches();
                    response = "Update time set to "
                                    + unit.toSeconds(timeout) + " seconds";
                }

                if (run) {
                    CellDataCachingService.this.notifyAll();

                    if (!response.isEmpty()) {
                        response += "\n";
                    }

                    response += "Update started.";
                }
            }
            return response;
        }
    }

    protected C collector;

    /**
     * <p>For diagnostic information.</p>
     */
    private long timeUsed;
    private long processCounter;

    @Override
    public Date getDate(String datetime) throws ParseException {
        if (datetime == null) {
            return null;
        }

        DateFormat format = new SimpleDateFormat(DATETIME_FORMAT);

        return format.parse(datetime);
    }

    @Override
    public void initialize() {
        rebuildCaches();
        collector.initialize(timeout, timeoutUnit);
        super.initialize();
    }

    /**
     * <p>Periodically extracts data concerning the transfers.
     * Can be notified in order to force an immediate collection,
     * but interruption will cause an exit.</p>
     */
    @Override
    public void run() {
        try {
            while (!Thread.interrupted()) {
                try {
                    processCounter++;
                    long start = System.currentTimeMillis();

                    refresh();

                    timeUsed = System.currentTimeMillis() - start;
                } catch (RuntimeException ee) {
                    LOGGER.error(ee.toString(), ee);
                }

                synchronized (this) {
                    wait(timeoutUnit.toMillis(timeout));
                }
            }
        } catch (InterruptedException e) {
            LOGGER.trace("Data collector interrupted");
        }
    }

    public void setCollector(C collector) {
        this.collector = collector;
    }

    @Override
    public void shutdown() {
        super.shutdown();
        collector.shutdown();
    }

    /**
     * <p>If the implementation uses a cache with an eviction protocol,
     * then this method should be overridden to reset the cache
     * configuration.</p>
     *
     * <p>Called under synchronization on the class instance.</p>
     */
    protected void rebuildCaches() {
        // NOP
    }

    /**
     * <p>Typically, this will be a two-step procedure involving
     * first the invocation of the collector method(s), and then
     * an update of the internal cache(s).</p>
     */
    protected abstract void refresh();
}
