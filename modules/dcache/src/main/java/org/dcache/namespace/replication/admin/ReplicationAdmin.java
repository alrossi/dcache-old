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
package org.dcache.namespace.replication.admin;

import com.google.common.base.Joiner;

import java.io.File;
import java.io.FileWriter;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import diskCacheV111.util.PnfsId;
import dmg.cells.nucleus.CellCommandListener;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import dmg.util.command.Option;
import org.dcache.auth.Subjects;
import org.dcache.namespace.replication.ReplicationHub;
import org.dcache.namespace.replication.ResilienceWatchdog;
import org.dcache.namespace.replication.data.PoolGroupInfo;
import org.dcache.namespace.replication.db.LocalNamespaceAccess;
import org.dcache.namespace.replication.monitoring.ActivityRegistry;
import org.dcache.namespace.replication.tasks.ProcessPnfsId;
import org.dcache.namespace.replication.tasks.ProcessPool;
import org.dcache.namespace.replication.tasks.ReplicaTaskInfo;

/**
 * Commands include:
 * <br>
 * <ol>
 *     <li>
 *         adjust replicas of a given pnfsid, or for all pnfisds on
 *         a given pool.
 *     </li>
 *     <li>
 *         cancel a running job.
 *     </li>
 *     <li>
 *         list pool group information for a given pool.
 *     </li>
 *     <li>
 *         list locations for a pnfsid.
 *     </li>
 *     <li>list pnfsids and counts on a given pool (useful for spot checking
 *          replica consistency).
 *     </li>
 *     <li>
 *         list running jobs.
 *     </li>
 *     <li>
 *         list cumulative totals for messages and jobs.
 *     </li>
 *     <li>
 *         refresh from pool monitor all pool group info currently
 *         held in cache.
 *     </li>
 *     <li>
 *         control the periodic scan of the resilient pools.
 *     </li>
 * </ol>
 *
 * Created by arossi on 1/31/15.
 */
public final class ReplicationAdmin implements CellCommandListener {

    enum WatchdogMode {
        ON, OFF, RUN, CANCEL, INFO;

        public static WatchdogMode parseOption(String option) {
            switch(option.toUpperCase()) {
                case "ON":
                    return WatchdogMode.ON;
                case "OFF":
                    return WatchdogMode.OFF;
                case "RUN":
                    return WatchdogMode.RUN;
                case "CANCEL":
                    return WatchdogMode.CANCEL;
                case "INFO":
                    return WatchdogMode.INFO;
                default:
                    throw new IllegalArgumentException("Unrecognized "
                                    + "option: '" + option + "'.");
            }
        }
    }

    private LocalNamespaceAccess access;
    private ReplicationHub hub;
    private ResilienceWatchdog watchdog;

    @Command(name = "rp adjust",
                    hint = "Launches an operation either on a single pnfisd "
                                    + "or on an entire pool.",
                    description = "A check will be run to see that the "
                                    + "number of replicas is properly "
                                    + "constrained between the appropriate "
                                    + "minimum and maximum, creating new "
                                    + "copies or removing redundant ones "
                                    + "as necessary.")
    class ReplicasAdjustCommand implements Callable<String> {
        @Option(name = "pnfsid",
                        usage = "Single pnfsid for which to run the adjustment.")
        String pnfsid;

        @Option(name = "pool",
                        usage = "Pool on which to conduct the adjustment of "
                                        + "all pnfsids.")
        String pool;

        @Override
        public String call() throws Exception {
            PoolGroupInfo info = null;

            if (pnfsid != null ) {
                if (pool != null) {
                    return "Please choose only one of the two options "
                                    + "(pnfsid or pool).";
                }

                PnfsId pnfsId = new PnfsId(pnfsid);
                Collection<String> pools
                                = access.getCacheLocation(Subjects.ROOT,
                                new PnfsId(pnfsid));
                if (pools.isEmpty()) {
                    return "Cannot find " + pnfsid + " at any location.";
                }

                String source = null;

                for (String pool: pools) {
                    info = hub.getPoolInfoCache().getPoolGroupInfo(pool);
                    if (!info.isResilient()) {
                        continue;
                    }
                    source = pool;
                    break;
                }

                if (source == null) {
                    return pnfsid + " does not reside on a resilient pool.";
                }

                new ProcessPnfsId(new ReplicaTaskInfo(pnfsId, pool), hub).launch();
                return "A pnfs update job has been started for "
                                + pnfsid + " on " + pool;
            }

            if (pool == null) {
                return "Please choose one of the two options (pnfsid or pool).";
            }

            info = hub.getPoolInfoCache().getPoolGroupInfo(pool);
            if (!info.isResilient()) {
                return pool + " is not resilient.";
            }

            new ProcessPool(new ReplicaTaskInfo(pool), hub).launch();
            return "A pool scan job has been started on " + pool;
        }
    }

    @Command(name = "rp cancel",
                    hint = "Cancel a running job.",
                    description = "A pnfsid as argument cancels a pnfsid update job; "
                                    + "a pool name cancels a pool status update job; "
                                    + "to stop a watchdog check, use the cancel "
                                    + "option on rp watchdog.")
    class CancelCommand implements Callable<String> {
        @Argument(index = 0,
                        required = true,
                        usage = "Either the pool name or the pnfsid "
                                        + "associated with this job.")
        String name;

        @Override
        public String call() throws Exception {
            try {
                hub.getRegistry().cancel(name);
            } catch (Exception e) {
                Throwable cause = e.getCause();
                return "Could not cancel any job associated with " + name + ": "
                                + e.getMessage()
                                + (cause == null ? "" : cause.getMessage());
            }
            return "Cancelled job for " + name;
        }
    }

    @Command(name = "rp ls info",
                    hint = "List pool group info for the pool.",
                    description = "Gets or loads cached metadata.  Only "
                                    + "valid for a resilient pool.")
    class LsInfoCommand implements Callable<String> {
        @Argument(required = true, usage = "Specify the pool name.")
        String pool;

        @Override
        public String call() throws Exception {
            PoolGroupInfo info = hub.getPoolInfoCache().getPoolGroupInfo(pool);
            StringBuilder builder = new StringBuilder();
            info.getInfo(builder);
            return builder.toString();
        }
    }

    @Command(name = "rp ls loc",
                    hint = "List all pool locations for a given pnfsid.",
                    description = "Issues a query to the namespace; "
                                    + "similar to cacheinfoof.")
    class LsLocationsCommand implements Callable<String> {
        @Argument(required = true, usage = "Specify the file pnfsid.")
        String pnfsid;

        @Override
        public String call() throws Exception {
           return Joiner.on(", ")
                        .join(access.getCacheLocation(Subjects.ROOT,
                                        new PnfsId(pnfsid)));
        }
    }

    @Command(name = "rp ls pnfsids",
                    hint = "List all pnfsids for a pool.",
                    description = "Issues a query to the namespace; "
                                    + "results can be written to a file.")
    class LsPnfsidsCommand implements Callable<String> {

        @Option(name = "out",
                        usage = "An output file path to which to write the "
                                        + "result.")
        String output;

        @Argument(required = true, usage = "Specify the pool name.")
        String pool;

        @Override
        public String call() throws Exception {
            try {
                Collection<String> results = access.getAllPnfsidsFor(pool);
                StringBuilder builder = new StringBuilder();
                for (String result: results) {
                    builder.append(result).append("\n");
                }
                builder.insert(0, "PnfsIds on " + pool).append(":\n\n");
                if (output != null) {
                    FileWriter fw = new FileWriter(new File(output), false);
                    fw.write(builder.toString());
                    fw.flush();
                    fw.close();
                }
                return builder.toString();
            } catch (Exception e) {
                e.printStackTrace();
                throw e;
            }
        }
    }

    @Command(name = "rp ls counts",
        hint = "List pnfsids and replica counts for a pool.",
        description = "Issues a query to the namespace; "
                        + "results can be written to a file.")
        class LsReplicaCountsCommand implements Callable<String> {

            @Option(name = "filter",
                            usage = "Filter the count using an inequality "
                                            + "predicate, such as '> 4', '< 2', "
                                            + "'= 3'.")
            String filter;

            @Option(name = "out",
                            usage = "An output file path to which to write the "
                                            + "result.")
            String output;

            @Argument(required = true, usage = "Specify the pool name.")
            String pool;

        @Override
        public String call() throws Exception {
            try {
                StringBuilder builder = new StringBuilder();
                Map<String, Integer> results = null;
                if (filter != null) {
                    results = access.getPnfsidCountsFor(pool, filter);
                } else {
                    results = access.getPnfsidCountsFor(pool);
                }

                for (String pnfsid: results.keySet()) {
                    builder.append(pnfsid).append("\t").append(
                                    results.get(pnfsid)).append("\n");
                }

                builder.insert(0, "PnfsIds on " + pool).append(":\n\n");
                if (output != null) {
                    FileWriter fw = new FileWriter(new File(output), false);
                    fw.write(builder.toString());
                    fw.flush();
                    fw.close();
                }
                return builder.toString();
            } catch (Exception e) {
                e.printStackTrace();
                throw e;
            }
        }
    }

    @Command(name = "rp ls running",
                    hint = "List the running operations.",
                    description = "Accesses the registry for a listing of "
                                    + "operation types on pools or pnfsids "
                                    + "in progress, sorted by start time.")
    class LsRunningCommand implements Callable<String> {

        @Option(name = "before",
                        usage = "List only activity begun before this 24-hour "
                                        + "date-time (yyyy/MM/dd HH:mm:ss).")
        String before;

        @Option(name = "after",
                        usage = "List only activity begun after this 24-hour "
                                        + "date-time (yyyy/MM/dd HH:mm:ss).")
        String after;

        @Option(name = "pools",
                        usage = "List only activity on this comma-delimited "
                                        + "list of pools).")
        String pools;

        @Option(name = "pnfsids",
                        usage = "List only activity for this comma-delimited "
                                        + "list of pnfsids).")
        String pnfsids;

        @Option(name = "types",
                        valueSpec = "pnfsid | pool-restart | pool-down | scan ",
                        usage = "List only activity for this comma-delimited "
                                        + "list of operation types).")
        String types;

        @Option(name = "limit",
                        usage = "Maximum number of entries to list.")
        String limit;

        @Override
        public String call() throws Exception {
            int max = Integer.MAX_VALUE;

            if (limit != null) {
                max = Integer.parseInt(limit);
            }

            return hub.getRegistry()
                            .list(before, after, pools, pnfsids, types, max);
        }
    }

    @Command(name = "rp ls totals",
                    hint = "List the total number of messages received and "
                                    + "operations performed since last start "
                                    + "of the replica manager.",
                    description = "Accesses the registry for the counters "
                                    + "and lists them by type.")
    class LsTotalsCommand implements Callable<String> {
        @Override
        public String call() throws Exception {
            return hub.getRegistry().printTotals();
        }
    }

    @Command(name = "rp reload",
                    hint = "Forces a reload of all the pool group info entries "
                                    + "currently held in cache.",
                    description = "Loads cached metadata from pool monitor. "
                                    + "This is useful after a dynamic reload "
                                    + "of the poolmanager.conf settings.")
    class ReloadCommand implements Callable<String> {
        @Override
        public String call() throws Exception {
            hub.getPoolInfoCache().reload();
            return "Pool group cached info has been refreshed.";
        }
    }

    @Command(name = "rp watchdog",
                    hint = "Control the periodic check of active resilient pools.",
                    description = "Activate, deactivate, or reschedule the periodic check "
                                    + "of active pools for deficient and excessive copies.")
    class WatchdogCommand implements Callable<String> {
        @Argument(index = 0,
                        valueSpec = "off|on|info|run|cancel ",
                        required = true,
                        usage = "off = turn the watchdog off; on = turn the watchdog on; "
                                        + "info = show when the next check is scheduled "
                                        + "to run; run = override the currently scheduled "
                                        + "check to run when indicated (default means now); "
                                        + "automatic periodic checking resumes afterwards, "
                                        + "with a new next time being scheduled using the "
                                        + "default timeout period; cancel = attempt to "
                                        + "cancel the currently running check.")
        String operation;

        @Argument(index = 1, required = false,
                        usage = "Specify when to run the check using "
                                        + "the format 'yyyy/MM/dd HH:mm:ss'.")
        String next;

        @Override
        public String call() throws Exception {
            WatchdogMode mode = WatchdogMode.valueOf(operation.toUpperCase());
            switch(mode) {
                case INFO:
                    if (!watchdog.isRunning()) {
                        return "Watchdog is off.";
                    }
                    return "Next scan: " + new Date(watchdog.getNextScan());
                case OFF:
                    if (watchdog.isRunning()) {
                        watchdog.shutdown();
                        return "Shut down watchdog.";
                    }
                    return "Watchdog already off.";
                case ON:
                    if (!watchdog.isRunning()) {
                        long time = System.currentTimeMillis()
                                        + watchdog.getTimeoutUnit()
                                        .toMillis(watchdog.getTimeout());
                        watchdog.reschedule(time, TimeUnit.MILLISECONDS);
                        watchdog.initialize();
                        return "Watchdog is on, next check scheduled for "
                                        + new Date(time).toString() + ".";
                    }
                    return "Watchdog already on.";
                case RUN:
                    if (!watchdog.isRunning()) {
                        return "Watchdog is off; turn it on first.";
                    }

                    long time;
                    if (next == null) {
                        time = System.currentTimeMillis() - 1000;
                    } else {
                        time = ActivityRegistry.DATE_FORMAT.parse(next).getTime();
                    }

                    watchdog.reschedule(time, TimeUnit.MILLISECONDS);
                    return "Check rescheduled for "
                                    + new Date(time).toString() + ".";
                case CANCEL:
                    watchdog.cancel();
                    return "Cancel called on watchdog. Next check scheduled for "
                                    + new Date(watchdog.getNextScan()).toString()
                                    + ".";
            }

            return "Next check scheduled for "
                            + new Date(watchdog.getNextScan()).toString()  + ".";
        }
    }

    public void setAccess(LocalNamespaceAccess access) {
        this.access = access;
    }

    public void setHub(ReplicationHub hub) {
        this.hub = hub;
    }

    public void setWatchdog(ResilienceWatchdog watchdog) {
        this.watchdog = watchdog;
    }
}
