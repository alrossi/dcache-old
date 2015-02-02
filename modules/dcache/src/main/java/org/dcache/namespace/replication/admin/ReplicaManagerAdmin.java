package org.dcache.namespace.replication.admin;

import com.google.common.base.Joiner;
import com.google.common.collect.Multimap;

import java.io.File;
import java.io.FileWriter;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import diskCacheV111.util.PnfsId;
import dmg.cells.nucleus.CellCommandListener;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import dmg.util.command.Option;
import org.dcache.auth.Subjects;
import org.dcache.namespace.replication.ReplicaManagerHub;
import org.dcache.namespace.replication.ResilienceWatchdog;
import org.dcache.namespace.replication.data.PoolGroupInfo;
import org.dcache.namespace.replication.db.LocalNamespaceAccess;
import org.dcache.namespace.replication.monitoring.ActivityRegistry;
import org.dcache.namespace.replication.workers.PnfsUpdateWorker;
import org.dcache.namespace.replication.workers.PoolScanWorker;

/**
 * Commands include:<br>
 * <ol>
 *     <li>
 *         adjust replicas of a given pnfsid, or for all pnfisds on
 *         a given pool.
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
 *         list running operations.
 *     </li>
 *     <li>
 *         list cumulative totals for messages and operations.
 *     </li>
 *     <li>
 *         refresh from pool monitor all pool group info currently
 *         held in cache.
 *     </li>
 *     <li>
 *         control the periodic scan of the resilient pools.
 *     </li>
 *     <li>
 *         turn processing of POOL RESTART messages on or off.
 *     </li>
 * </ol>
 * Created by arossi on 1/31/15.
 */
public final class ReplicaManagerAdmin implements CellCommandListener {

    enum WatchdogMode {
        ON, OFF, RUN, INFO;

        public static WatchdogMode parseOption(String option) {
            switch(option.toUpperCase()) {
                case "ON":
                    return WatchdogMode.ON;
                case "OFF":
                    return WatchdogMode.OFF;
                case "RUN":
                    return WatchdogMode.RUN;
                case "INFO":
                    return WatchdogMode.INFO;
                default:
                    throw new IllegalArgumentException("Unrecognized "
                                    + "option: '" + option + "'.");
            }
        }
    }

    private LocalNamespaceAccess access;
    private ReplicaManagerHub hub;
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

                pool = pools.iterator().next();
                new Thread(new PnfsUpdateWorker(pool, pnfsId, hub)).start();
                return "A pnfs update job has been started for "
                                + pnfsid + " on " + pool;
            }

            if (pool == null) {
                return "Please choose one of the two options (pnfsid or pool).";
            }

            new Thread(new PoolScanWorker(pool, Collections.EMPTY_SET, hub))
                            .start();
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
                    description = "Issues a query to the namespace.")
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
                Multimap<String, Integer> results = null;
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
                    description = "Activate, turn off, or reschedule the periodic check "
                                    + "of active pools for deficient and excessive copies.")
    class WatchdogCommand implements Callable<String> {
        @Argument(index = 0,
                        valueSpec = "off|on|info|run ",
                        required = true,
                        usage = "off = turn the watchdog off; on = turn the watchdog on; "
                                        + "info = show when the next check is scheduled "
                                        + "to run; run = override the currently scheduled "
                                        + "check to run when indicated (default means now); "
                                        + "automatic periodic checking resumes afterwards, "
                                        + "with a new next time being scheduled using the "
                                        + "default timeout period.")
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
            }

            return "Next check scheduled for "
                            + new Date(watchdog.getNextScan()).toString()  + ".";
        }
    }

    public void setAccess(LocalNamespaceAccess access) {
        this.access = access;
    }

    public void setHub(ReplicaManagerHub hub) {
        this.hub = hub;
    }

    public void setWatchdog(ResilienceWatchdog watchdog) {
        this.watchdog = watchdog;
    }
}
