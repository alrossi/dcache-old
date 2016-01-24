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
package org.dcache.resilience.admin;

import java.io.File;
import java.io.FileWriter;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import diskCacheV111.util.PnfsId;
import dmg.cells.nucleus.CellCommandListener;
import dmg.util.command.Argument;
import dmg.util.command.Option;
import org.dcache.resilience.data.MessageType;
import org.dcache.resilience.data.PnfsFilter;
import org.dcache.resilience.data.PnfsOperation;
import org.dcache.resilience.data.PnfsOperationMap;
import org.dcache.resilience.data.PnfsUpdate;
import org.dcache.resilience.data.PoolFilter;
import org.dcache.resilience.data.PoolInfoMap;
import org.dcache.resilience.data.PoolOperationMap;
import org.dcache.resilience.db.NamespaceAccess;
import org.dcache.resilience.handlers.PnfsOperationHandler;
import org.dcache.resilience.util.MapInitializer;
import org.dcache.resilience.util.MessageGuard;
import org.dcache.resilience.util.OperationHistory;
import org.dcache.resilience.util.OperationStatistics;
import org.dcache.util.ExceptionMessage;
import org.dcache.vehicles.FileAttributes;

/**
 * <p>Collects all admin shell commands for convenience.  See further individual
 * command annotations for details.</p>
 *
 * <p>In order to be able to change the name of the commands via the annotation,
 * this class, which contains the actual command code, should be extended.</p>
 *
 * @see EmbeddedResilienceCommands
 * @see StandaloneResilienceCommands
 *
 * Created by arossi on 09/17/2015.
 */
public abstract class ResilienceCommands implements CellCommandListener {

    protected static final String HINT_CHECK
                    = "Launch an operation to adjust replicas for a "
                    + "single pnfsid.";
    protected static final String HINT_COUNT
                    = "List pnfsids and replica counts for a pool.";
    protected static final String HINT_DIAG
                    = "Print diagnostic statistics.";
    protected static final String HINT_DIAG_CLEAR
                    = "Reset pnfs operation timings to 0.";
    protected static final String HINT_DIAG_HIST
                    = "Print diagnostic history.";
    protected static final String HINT_DISABLE
                    = "Turn off replication handling.";
    protected static final String HINT_ENABLE
                    = "Turn on replication handling.";
    protected static final String HINT_GNAME
                    = "Find the name of a pool group given its key.";
    protected static final String HINT_HIST
                    = "Display a history of the most recent terminated "
                    + "pnfs operations.";
    protected static final String HINT_PNFSCNCL
                    = "Cancel pnfs operations.";
    protected static final String HINT_PNFS_CTRL
                    = "Control checkpointing or handling of pnfs operations.";
    protected static final String HINT_PNFSLS
                    = "List entries in the pnfs operation table.";
    protected static final String HINT_POOLCNCL
                    = "Cancel pool operations.";
    protected static final String HINT_POOL_CTRL
                    = "Control the periodic check of active resilient pools "
                    + "or processing of pool state changes.";
    protected static final String HINT_POOLINFO
                    = "List tags and dynamic info for a pool or pools.";
    protected static final String HINT_POOLLS
                    = "List entries in the pool operation table.";
    protected static final String HINT_PNAME
                    = "Find the name of a pool given its key.";
    protected static final String HINT_SCAN
                    = "Launch a scan of one or more pools.";
    protected static final String HINT_UNIQUE
                    = "List pnfsids on the given pools which have a "
                    + "replica count of 1.";
    protected static final String HINT_VERIFY
                    = "List the storage units linked to a pool group "
                                    + "and confirm resilience constraints "
                                    + "can be met by the member pools.";

    protected static final String DESC_CHECK
                    = "Run a check to see that the number of replicas is properly "
                                    + "constrained, creating new "
                                    + "copies or removing redundant ones "
                                    + "as necessary.";
    protected static final String DESC_PNFS_CTRL
                    = "Run checkpointing, reset checkpoint "
                                    + "properties, reset operation properties, "
                                    + "turn processing of operations on or off "
                                    + "(start/shutdown), or display info relevant "
                                    + "to operation processing and checkpointing.";
    protected static final String DESC_COUNT
                    = "Issues a query to the namespace; "
                                    + "results can be written to a file.";
    protected static final String DESC_DIAG
                    = "Lists the total number of messages received  "
                                    + "and operations performed since last start "
                                    + "of the resilience system.  Rate/sec is "
                                    + "sampled over the interval since the last "
                                    + "checkpoint. These values for new "
                                    + "location messages and pnfsid "
                                    + "operations are recorded "
                                    + "to a stastistics file located in the "
                                    + "resilience home directory, and which "
                                    + "can be displayed using the history option";
    protected static final String DESC_DIAG_TIMINGS
                    = "Begins a new sampling interval which "
                    + "lasts until the counters are reset again.";
    protected static final String DESC_DIAG_HIST
                    = "Reads in the contents of the diagnostic history file "
                                    + "recording periodic statics "
                                    + "(see diag command).";
    protected static final String DESC_DISABLE
                    = "Prevents messages from being processed by "
                                    + "the replication system.  "
                    + "To disable all internal operations; either use "
                    + "the two 'ctrl' commands individually, or "
                    + "the 'strict' argument to this command.";
    protected static final String DESC_ENABLE
                    = "Allows messages to be processed by "
                                    + "the replication system."
                    + "Will also (re-)enable all internal operations if they "
                    + "are not running.";
    protected static final String DESC_GNAME
                    = "Looks up the name of the pool group using the "
                                    + "index key.";
    protected static final String DESC_HIST
                    = "When pnfs operations complete or are aborted, their "
                                    + "string representations are added to a "
                                    + "circular buffer whose capacity is set "
                                    + "by the property "
                                    + "'pnfsmanager.resilience.history.buffer-size'.";
    protected static final String DESC_PNFSCNCL
                    = "Scans the pnfs table and cancels "
                                    + "operations matching the filter parameters. "
                                    + "NOTE: to cancel pnfs operations matching "
                                    + "source, target or parent pools, use "
                                    + "'pool cancel' with appropriate options.";
    protected static final String DESC_PNFSLS
                    = "Scans the table and returns operations matching "
                    + "the filter parameters.";
    protected static final String DESC_POOLCNCL
                    = "Scans the pool table and cancels "
                                    + "operations matching the filter parameters; "
                                    + "if 'includeParents' is true, also "
                                    + "scans the pnfs table.";
    protected static final String DESC_POOL_CTRL
                    = "Activates, deactivates, or resets the periodic checking "
                    + "of active pools; turns all pool state handling on or off "
                    + "(start/shutdown)";
    protected static final String DESC_POOLINFO
                    = "Does NOT refresh the cost info.";
    protected static final String DESC_POOLLS
                    = "Scans the table and returns "
                    + "operations matching the filter parameters.";
    protected static final String DESC_PNAME
                    = "Looks up the name of the pool "
                    + "using the index key.";
    protected static final String DESC_SCAN
                    = "A check will be initiated to see that the "
                                    + "number of replicas on the pool is "
                                    + "properly constrained, creating new "
                                    + "copies or removing redundant ones "
                                    + "as necessary. Note: will not override "
                                    + "a currently running operation.";
    protected static final String DESC_UNIQUE
                    = "An alias for counts <pool> -eq=1, "
                                    + "but with the ability to query multiple "
                                    + "pools at once.";
    protected static final String DESC_VERIFY
                    = "Tries to satisfy the constraints on replica "
                                    + "count and exclusivity tags for all the "
                                    + "storage units in the pool group by "
                                    + "attempting to assign the required number "
                                    + "of locations for a hypothetical file on"
                                    + "belonging to each.";

    private static final String FORMAT_STRING = "yyyy/MM/dd-HH:mm:ss";
    private static final DateFormat DATE_FORMAT
                    = new SimpleDateFormat(FORMAT_STRING);
    private static final String REQUIRE_LIMIT
                    = "The current table contains %s entries; listing them all "
                    + "could cause an out-of-memory error and cause the resilience "
                    + "system to fail and/or restart; if you wish to proceed "
                    + "with this listing, reissue the command with the explicit "
                    + "option '-limit=%s'";
    private static final long LS_THRESHOLD = 5000000L;

    enum ControlMode {
        ON,
        OFF,
        START,
        SHUTDOWN,
        RESET,
        RUN,
        INFO;

        public static ControlMode parseOption(String option) {
            switch (option.toUpperCase()) {
                case "ON":
                    return ControlMode.ON;
                case "OFF":
                    return ControlMode.OFF;
                case "START":
                    return ControlMode.START;
                case "SHUTDOWN":
                    return ControlMode.SHUTDOWN;
                case "RESET":
                    return ControlMode.RESET;
                case "RUN":
                    return ControlMode.RUN;
                case "INFO":
                    return ControlMode.INFO;
                default:
                    throw new IllegalArgumentException(
                                    "Unrecognized option: '" + option
                                                    + "'.");
            }
        }
    }

    enum SortOrder {
        ASC, DESC
    }

    abstract class ResilienceCommand implements Callable<String> {
        @Override
        public String call() throws Exception {
            String error = initializer.getInitError();

            if (error != null) {
                return error;
            }

            if (!initializer.isInitialized()) {
                return "Resilience waiting for initialization to stabilize; "
                                + "this may take a while, depending the size "
                                + "of the operation cache being reloaded, or "
                                + "whether pools are being initialized.";
            }

            return doCall();
        }

        protected abstract String doCall() throws Exception;
    }

    abstract class CheckCommand extends ResilienceCommand {
        @Argument(index = 0,
                        required = true,
                        usage = "Single pnfsid for which to run the adjustment.")
        String pnfsid;

        @Override
        protected String doCall() throws Exception {
            PnfsId pnfsId = new PnfsId(pnfsid);
            FileAttributes attr = namespaceAccess.getRequiredAttributes(pnfsId);
            Iterator<String> it = attr.getLocations().iterator();
            if (!it.hasNext()) {
                return pnfsid + " does not seem to have any locations.";
            }
            String pool = it.next();
            PnfsUpdate update
                            = new PnfsUpdate(pnfsId, pool, MessageType.NEW_FILE_LOCATION);
            pnfsOperationHandler.handleLocationUpdate(update);
            return "An adjustment activity has been started for " + pnfsId
                            + " from source pool " + pool + ".";
        }
    }

    abstract class CountsCommand extends ResilienceCommand {
        @Option(name = "eq",
                        usage = "Filter on '=' this value.")
        String eq;

        @Option(name = "gt",
                        usage = "Filter on '>' this value.")
        String gt;

        @Option(name = "lt",
                        usage = "Filter on '<' this value.")
        String lt;

        @Option(name = "out",
                        usage = "An output file path to which to write the "
                                        + "result.")
        String output;

        @Argument(required = true,
                        usage = "Specify the pool name.") String pool;

        @Override
        protected String doCall() throws Exception {
            try {
                StringBuilder builder = new StringBuilder();
                Map<String, Integer> results = null;

                if (eq != null) {
                    results = namespaceAccess.getPnfsidCountsFor(pool,
                                    "=" + eq);
                } else if (gt != null) {
                    results = namespaceAccess.getPnfsidCountsFor(pool,
                                    ">" + gt);
                } else if (lt != null) {
                    results = namespaceAccess.getPnfsidCountsFor(pool,
                                    "<" + lt);
                } else {
                    results = namespaceAccess.getPnfsidCountsFor(pool);
                }

                for (Entry<String, Integer> entry : results.entrySet()) {
                    builder.append(entry.getKey()).append("\t")
                                    .append(entry.getValue()).append("\n");
                }

                if (output != null) {
                    FileWriter fw = new FileWriter(new File(output), false);
                    fw.write(builder.toString());
                    fw.flush();
                    fw.close();
                }

                builder.append("\nPNFSIDS:\t" + results.size() + "\n\n");
                return builder.toString();
            } catch (Exception e) {
                return new ExceptionMessage(e).toString();
            }
        }
    }

    abstract class DiagCommand extends ResilienceCommand {
        @Argument(required = false,
                        valueSpec = "regular expression; "
                                        + "default prints only summary info",
                        usage = "Include pools matching this expression.")
        String pools;

        @Override
        protected String doCall() throws Exception {
            return counters.print(pools);
        }
    }

    abstract class DiagClearTimingsCommand extends ResilienceCommand {
        @Override
        protected String doCall() throws Exception {
            counters.clearTimings();
            return "Reset pnfs operation timers.";
        }
    }

    abstract class DiagHistoryCommand extends ResilienceCommand {
        @Option(name = "limit",
                        usage = "Display up to this number of lines.")
        String limit;

        @Option(name = "order",
                        valueSpec = "asc | desc (default)",
                        usage = "Display lines in ascending or "
                                        + "descending order of timestamp.")
        String order = "desc";

        protected String doCall() throws Exception {
            SortOrder order = SortOrder.valueOf(this.order.toUpperCase());
            Integer count = limit == null ? null : Integer.valueOf(limit);
            return counters.readStatistics(count, order == SortOrder.DESC);
        }
    }

    abstract class DisableCommand extends ResilienceCommand {
        @Argument(required = false,
                        usage = "Whether to shutdown all operations "
                                        + "(without this argument, only "
                                        + "incoming messages are blocked).",
                        valueSpec = "strict")
        String strict;

        @Override
        protected String doCall() throws Exception {
            String message = "Processing of incoming messages has been disabled.";
            if (strict != null) {
                if (!"strict".equals(strict.toLowerCase())) {
                    return "Unrecognized argument " + strict;
                }

                if (pnfsOperationMap.isRunning()) {
                    pnfsOperationMap.shutdown();
                }

                if (poolOperationMap.isRunning()) {
                    poolOperationMap.shutdown();
                }

                message = "All resilience operations have been shutdown.";
            }

            messageGuard.setEnabled(false);
            return message;
        }
    }

    abstract class EnableCommand extends ResilienceCommand {
        @Override
        protected String doCall() throws Exception {
            if (!initializer.initialize()) {
                messageGuard.setEnabled(true);

                if (!poolOperationMap.isRunning()) {
                    poolOperationMap.loadPools();
                    poolOperationMap.initialize();
                }

                if (!pnfsOperationMap.isRunning()) {
                    pnfsOperationMap.initialize();
                    pnfsOperationMap.reload();
                }

                return "Resilience system has been re-enabled.";
            }
            return "Resilience system has not yet been initialized.";
        }
    }

    abstract class GroupNameCommand extends ResilienceCommand {
        @Argument(required = true, usage = "Integer key.")
        String key;

        @Override
        protected String doCall() throws Exception {
            try {
                return poolInfoMap.getGroup(Integer.parseInt(key));
            } catch (IndexOutOfBoundsException | NoSuchElementException e) {
                return "No such pool group: " + key;
            }
        }
    }

    abstract class PnfsControlCommand extends ResilienceCommand {
        @Argument(index = 0,
                        valueSpec = "off|on|info|reset|start|shutdown|run ",
                        required = false,
                        usage = "off = turn checkpointing off; "
                                        + "on = turn checkpointing on; "
                                        + "info = information (default); "
                                        + "reset = reset properties; "
                                        + "start = (re)start processing of pnfs operations; "
                                        + "shutdown = stop all processing of pnfs operations; "
                                        + "run = checkpoint to disk immediately." )
        String arg = "INFO";

        @Option(name = "checkpoint",
                        usage = "With reset mode (one of checkpoint|sweep). "
                                        + "Interval length between checkpointing "
                                        + "of the pnfs operation data.")
        String checkpoint;

        @Option(name = "sweep",
                        usage = "With reset mode (one of checkpoint|sweep). "
                                        + "Minimal interval between sweeps of "
                                        + "the pnfs operations.")
        String sweep;

        @Option(name = "unit",
                        valueSpec = "SECONDS|MINUTES|HOURS ",
                        usage = "Checkpoint or sweep interval unit.")
        String unit;

        @Option(name = "retries",
                        usage = "Maximum number of retries on a failed operation.")
        String retries;

        @Option(name = "file",
                        usage = "Alternate (full) path for checkpoint file.")
        String file;

        @Override
        protected String doCall() throws Exception {
            ControlMode mode = ControlMode.valueOf(arg.toUpperCase());

            TimeUnit timeUnit = null;
            if (unit != null) {
                timeUnit = TimeUnit.valueOf(unit);
            }

            switch (mode) {
                case START:
                    if (pnfsOperationMap.isRunning()) {
                        return "Consumer is already running.";
                    }
                    pnfsOperationMap.initialize();
                    pnfsOperationMap.reload();
                    return "Consumer initialized and checkpoint file reloaded.";
                case SHUTDOWN:
                    if (!pnfsOperationMap.isRunning()) {
                        return "Consumer is not running.";
                    }
                    pnfsOperationMap.shutdown();
                    return "Consumer has been shutdown.";
                case OFF:
                    if (pnfsOperationMap.isCheckpointingOn()) {
                        pnfsOperationMap.stopCheckpointer();
                        return "Shut down checkpointing.";
                    }
                    return "Checkpointing already off.";
                case ON:
                    if (!pnfsOperationMap.isCheckpointingOn()) {
                        pnfsOperationMap.startCheckpointer();
                        return infoMessage();
                    }
                    return "Checkpointing already on.";
                case RUN:
                    if (!pnfsOperationMap.isCheckpointingOn()) {
                        return "Checkpointing is off; please turn it on first.";
                    }
                    pnfsOperationMap.runCheckpointNow();
                    return "Forced checkpoint.";
                case RESET:
                    if (!pnfsOperationMap.isCheckpointingOn()) {
                        return "Checkpointing is off; please turn it on first.";
                    }

                    if (checkpoint != null) {
                        pnfsOperationMap.setCheckpointExpiry(Integer.parseInt(checkpoint));
                        if (timeUnit != null) {
                            pnfsOperationMap.setCheckpointExpiryUnit(timeUnit);
                        }
                    } else if (sweep != null) {
                        pnfsOperationMap.setTimeout(Integer.parseInt(sweep));
                        if (timeUnit != null) {
                            pnfsOperationMap.setCheckpointExpiryUnit(timeUnit);
                        }
                    }

                    if (retries != null) {
                        pnfsOperationMap.setMaxRetries(Integer.parseInt(retries));
                    }

                    if (file != null) {
                        pnfsOperationMap.setCheckpointFilePath(file);
                    }

                    pnfsOperationMap.reset();
                    // fall through here
                case INFO:
                default:
                    return infoMessage();
            }
        }

        private String infoMessage() {
            StringBuilder info = new StringBuilder();
            info.append(String.format("maximum concurrent operations %s.\n"
                                            + "maximum retries on failure %s.\n",
                            pnfsOperationMap.getMaxRunning(),
                            pnfsOperationMap.getMaxRetries()));
            info.append(String.format("checkpoint interval %s %s.\n"
                                            + "checkpoint file path %s.\n",
                            pnfsOperationMap.getCheckpointExpiry(),
                            pnfsOperationMap.getCheckpointExpiryUnit(),
                            pnfsOperationMap.getCheckpointFilePath()));
            counters.getCheckpointInfo(info);
            return info.toString();
        }
    }

    abstract class PnfsOpHistoryCommand extends ResilienceCommand {
        @Argument(required = false,
                        valueSpec = "errors",
                        usage = "Display just the failures")
        String errors;

        @Option(name = "limit",
                        usage = "Display up to this number of entries.")
        String limit;

        @Option(name = "order",
                        valueSpec = "asc | desc (default)",
                        usage = "Display entries in ascending or descending order of arrival.")
        String order = "desc";

        @Override
        protected String doCall() throws Exception {
            boolean failed = false;
            if (errors != null) {
                if (!"errors".equals(errors)) {
                    return  "Optional argument must be 'errors'";
                }
                failed = true;
            }

            SortOrder order = SortOrder.valueOf(this.order.toUpperCase());

            switch (order) {
                case ASC:
                    if (limit != null) {
                        return history.ascending(failed, Integer.parseInt(limit));
                    }
                    return history.ascending(failed);
                case DESC:
                default:
                    if (limit != null) {
                        return history.descending(failed, Integer.parseInt(limit));
                    }
                    return history.descending(failed);
            }
        }
    }

    abstract class PnfsOpCancelCommand extends ResilienceCommand {

        @Option(name = "state",
                        valueSpec = "WAITING | RUNNING "
                                        + "(default is RUNNING) ",
                        usage = "Cancel operations for pnfsids matching this "
                                        + "comma-delimited set of operation states.")
        String state = "WAITING,RUNNING";

        @Option(name = "forceRemoval",
                        valueSpec = "true | false (default) ",
                        usage = "Remove all waiting operations for this match "
                                        + "after cancellation of the running tasks. "
                                        + "(This option is redundant if the "
                                        + "state includes WAITING.)")
        boolean forceRemoval = false;

        @Option(name = "lastUpdateBefore",
                        valueSpec = FORMAT_STRING,
                        usage = "Cancel only operations whose last "
                                        + "update was before this date-time.")
        String lastUpdateBefore;

        @Option(name = "lastUpdateAfter",
                        valueSpec = FORMAT_STRING,
                        usage = "Cancel only operations whose last "
                                        + "update was after this date-time.")
        String lastUpdateAfter;

        @Option(name = "retentionPolicy",
                        valueSpec = "REPLICA | CUSTODIAL ",
                        usage = "Cancel only operations for pnfsids with this "
                                        + "policy.")
        String retentionPolicy;

        @Option(name = "storageUnit",
                        usage = "Cancel only operations for pnfsids with this "
                                        + "storage unit/group.")
        String storageUnit;

        @Option(name = "opCount",
                        usage = "Operation count of the operation.")
        String opCount;

        @Option(name = "parent",
                        usage = "Operation parent pool name.")
        String parent;

        @Option(name = "source",
                        usage = "Operation source pool name.")
        String source;

        @Option(name = "target",
                        usage = "Operation target pool name.")
        String target;

        @Argument(required = false,
                        valueSpec = "Use '*' to cancel all operations.",
                        usage = "Cancel activities for this comma-delimited "
                                        + "list of pnfsids.")
        String pnfsids;

        @Override
        protected String doCall() throws Exception {
            PnfsFilter filter = new PnfsFilter();

            if (!"*".equals(pnfsids)) {
                filter.setLastUpdateBefore(getTimestamp(lastUpdateBefore));
                filter.setLastUpdateAfter(getTimestamp(lastUpdateAfter));
                filter.setPnfsIds(pnfsids);
                filter.setRetentionPolicy(retentionPolicy);
                filter.setStorageUnit(storageUnit);
                filter.setParent(parent);
                filter.setSource(source);
                filter.setTarget(target);

                if (opCount != null) {
                    filter.setOpCount(Short.valueOf(opCount));
                }

                if (filter.isUndefined()) {
                    return "Please set at least one option on the filter.";
                }

                if (filter.isSimplePnfsMatch()) {
                    pnfsOperationMap.cancel(new PnfsId(pnfsids), forceRemoval);
                    return String.format("Issued cancel command for %s.", pnfsids);
                }
            }

            forceRemoval |= state.toUpperCase().contains("WAITING");

            filter.setState(state);
            filter.setForceRemoval(forceRemoval);

            return String.format("Issued cancel command to %s pnfs operations.",
                                 pnfsOperationMap.cancel(filter));
        }
    }

    abstract class PnfsOpLsCommand extends ResilienceCommand {
        @Option(name = "retentionPolicy",
                        valueSpec = "REPLICA | CUSTODIAL ",
                        usage = "List only operations for pnfsids with this "
                                        + "policy.")
        String retentionPolicy;

        @Option(name = "storageUnit",
                        usage = "List only operations for pnfsids with this "
                                        + "storage unit/group.")
        String storageUnit;

        @Option(name = "state",
                        valueSpec = "WAITING | RUNNING "
                                        + "(default is both) ",
                        usage = "List only operations for pnfsids matching this "
                                        + "comma-delimited set of operation states.")
        String state = "WAITING,RUNNING";

        @Option(name = "lastUpdateBefore",
                        valueSpec = FORMAT_STRING,
                        usage = "List only operations whose last "
                                        + "update was before this date-time.")
        String lastUpdateBefore;

        @Option(name = "lastUpdateAfter",
                        valueSpec = FORMAT_STRING,
                        usage = "List only operations whose last "
                                        + "update was after this date-time.")
        String lastUpdateAfter;

        @Option(name = "opCount",
                        usage = "Operation count of the operation.")
        String opCount;

        @Option(name = "parent",
                        usage = "Operation parent pool name.")
        String parent;

        @Option(name = "source",
                        usage = "Operation source pool name.")
        String source;

        @Option(name = "target",
                        usage = "Operation target pool name.")
        String target;

        @Option(name = "limit",
                        usage = "Maximum number of rows to return.")
        String limit;

        @Argument(required = false,
                        valueSpec = "No argument lists all operations; "
                                        + "use '~' just to return the count.",
                        usage = "List only activities for this comma-delimited "
                                        + "list of pnfsids.")
        String pnfsids;

        @Override
        protected String doCall() throws Exception {
            boolean count = "~".equals(pnfsids);

            PnfsFilter filter = new PnfsFilter();
            filter.setLastUpdateBefore(getTimestamp(lastUpdateBefore));
            filter.setLastUpdateAfter(getTimestamp(lastUpdateAfter));
            filter.setState(state);
            filter.setRetentionPolicy(retentionPolicy);
            filter.setStorageUnit(storageUnit);
            filter.setParent(parent);
            filter.setSource(source);
            filter.setTarget(target);

            if (opCount != null) {
                filter.setOpCount(Short.valueOf(opCount));
            }

            if (!count) {
                filter.setPnfsIds(pnfsids);
            } else {
                return String.format("%s matching operations.\n",
                                pnfsOperationMap.count(filter));
            }

            if (filter.isSimplePnfsMatch()) {
                PnfsOperation op = pnfsOperationMap.getOperation(new PnfsId(pnfsids));
                if (op == null) {
                    return String.format("No operation currently registered for %s.",
                                    pnfsids);
                }
                return op.toString() + "\n";
            }

            long size = pnfsOperationMap.size();
            int limitValue = (int)size;

            if (limit == null) {
                if (size >= LS_THRESHOLD) {
                    return String.format(REQUIRE_LIMIT, size, size);
                }
            } else {
                limitValue = Integer.parseInt(limit);
            }

            return pnfsOperationMap.list(filter, limitValue);
        }
    }


    abstract class PoolControlCommand extends ResilienceCommand {
        @Argument(index = 0,
                        valueSpec = "off|on|start|shutdown|info|reset|run ",
                        required = false,
                        usage = "off = turn scanning off; on = turn scanning on; "
                                        + "shutdown = turn off all pool operations; "
                                        + "start = activate all pool operations; "
                                        + "info = show status of watchdog and scan window (default); "
                                        + "reset = reset properties; "
                                        + "run = interrupt current wait and do a sweep." )
        String operation = "INFO";

        @Option(name = "window",
                        usage = "With reset mode (one of window|sweep|down). "
                                        + "Amount of time which must pass since "
                                        + "the last scan of a pool for it to be "
                                        + "scanned again.")
        String window;

        @Option(name = "sweep",
                        usage = "With reset mode (one of window|sweep|down). "
                                        + "How often a sweep of the pool "
                                        + "operations is made.")
        String sweep;

        @Option(name = "down",
                        usage = "With reset mode (one of window|sweep|down). "
                                        + "Minimum grace period between reception "
                                        + "of a DOWN status message and scan of  "
                                        + "the given pool.")
        String down;

        @Option(name = "restarts",
                        valueSpec = "true | false (default)",
                        usage = "Whether or not to scan a pool when "
                                        + "a restart message is received.")
        String restart;

        @Option(name = "unit",
                        valueSpec = "SECONDS | MINUTES | HOURS | DAYS ",
                        usage = "For the sweep/window/down options.")
        String unit;

        @Override
        protected String doCall() throws Exception {
            ControlMode mode = ControlMode.valueOf(operation.toUpperCase());

            TimeUnit timeUnit = null;
            if (unit != null) {
                timeUnit = TimeUnit.valueOf(unit);
            }

            switch (mode) {
                case START:
                    if (poolOperationMap.isRunning()) {
                        return "Consumer is already running.";
                    }
                    poolOperationMap.loadPools();
                    poolOperationMap.initialize();
                    return "Consumer initialized and pools reloaded.";
                case SHUTDOWN:
                    if (!poolOperationMap.isRunning()) {
                        return "Consumer is not running.";
                    }
                    poolOperationMap.shutdown();
                    return "Consumer has been shutdown.";
                case OFF:
                    if (poolOperationMap.isWatchdogOn()) {
                        poolOperationMap.setWatchdog(false);
                        return "Shut down watchdog.";
                    }
                    return "Watchdog already off.";
                case ON:
                    if (!poolOperationMap.isWatchdogOn()) {
                        poolOperationMap.setWatchdog(true);
                        return infoMessage();
                    }
                    return "Watchdog already on.";
                case RUN:
                    if (!poolOperationMap.isWatchdogOn()) {
                        return "Watchdog is off; please turn it on first.";
                    }
                    poolOperationMap.runNow();
                    return "Forced watchdog scan";
                case RESET:
                    if (!poolOperationMap.isWatchdogOn()) {
                        return "Watchdog is off; please turn it on first.";
                    }

                    if (window != null) {
                        poolOperationMap.setRescanWindow(Integer.parseInt(window));
                        if (timeUnit != null) {
                            poolOperationMap.setRescanWindowUnit(timeUnit);
                        }
                    } else if (sweep != null) {
                        poolOperationMap.setTimeout(Integer.parseInt(sweep));
                        if (timeUnit != null) {
                            poolOperationMap.setTimeoutUnit(timeUnit);
                        }
                    } else if (down != null) {
                        poolOperationMap.setDownGracePeriod(Integer.parseInt(down));
                        if (timeUnit != null) {
                            poolOperationMap.setDownGracePeriodUnit(timeUnit);
                        }
                    }

                    if (restart != null) {
                        poolOperationMap.setHandleRestarts(Boolean.valueOf(restart));
                    }
                    poolOperationMap.reset();
                    // fall through here
                case INFO:
                default:
                    return infoMessage();
            }
        }

        private String infoMessage() {
            return String.format("down grace period %s %s\n"
                                            + "handle restarts %s\n"
                                            + "maximum concurrent operations %s\n"
                                            + "scan window set to %s %s.\n"
                                            + "period set to %s %s.\n",
                            poolOperationMap.getDownGracePeriod(),
                            poolOperationMap.getDownGracePeriodUnit(),
                            poolOperationMap.isHandleRestarts(),
                            poolOperationMap.getMaxConcurrentRunning(),
                            poolOperationMap.getScanWindow(),
                            poolOperationMap.getScanWindowUnit(),
                            poolOperationMap.getTimeout(),
                            poolOperationMap.getTimeoutUnit());
        }
    }

    abstract class PoolOpCancelCommand extends ResilienceCommand {
        @Option(name = "status",
                        valueSpec = "DOWN | RESTART ",
                        usage = "Cancel only operations on pools matching this "
                                        + "pool status.")
        String poolStatus;

        @Option(name = "lastUpdateBefore",
                        valueSpec = FORMAT_STRING,
                        usage = "Cancel only operations on pools whose last "
                                        + "update was before this date-time.")
        String lastUpdateBefore;

        @Option(name = "lastUpdateAfter",
                        valueSpec = FORMAT_STRING,
                        usage = "Cancel only operations on pools whose last "
                                        + "update was after this date-time.")
        String lastUpdateAfter;

        @Option(name = "lastScanBefore",
                        valueSpec = FORMAT_STRING,
                        usage = "Cancel only operations on pools whose scan "
                                        + "update was before this date-time.")
        String lastScanBefore;

        @Option(name = "lastScanAfter",
                        valueSpec = FORMAT_STRING,
                        usage = "Cancel only operations on pools whose scan "
                                        + "update was after this date-time.")
        String lastScanAfter;

        @Option(name = "includeParent",
                        valueSpec = "true | false (default) ",
                        usage = "Cancel pnfsId operations whose parents match "
                                        + "the pool pattern.")
        boolean includeParent = false;

        @Option(name = "includeSource",
                        valueSpec = "true | false (default) ",
                        usage = "Cancel pnfsId operations whose sources match "
                                        + "the pool pattern.")
        boolean includeSource = false;

        @Option(name = "includeTarget",
                        valueSpec = "true | false (default) ",
                        usage = "Cancel pnfsId operations whose targets match "
                                        + "the pool pattern.")
        boolean includeTarget = false;

        @Argument(required = false,
                        valueSpec = "regular expression; no argument is the same as '.*'",
                        usage = "Cancel only operations on pools matching this "
                                        + "expression.")
        String pools;

        @Override
        protected String doCall() throws Exception {
            PoolFilter filter = new PoolFilter();
            filter.setPool(pools);
            filter.setLastScanAfter(getTimestamp(lastScanAfter));
            filter.setLastScanBefore(getTimestamp(lastScanBefore));
            filter.setLastUpdateBefore(getTimestamp(lastUpdateBefore));
            filter.setLastUpdateAfter(getTimestamp(lastUpdateAfter));
            filter.setPoolStatus(poolStatus);
            filter.setParent(includeParent);
            filter.setSource(includeSource);
            filter.setTarget(includeTarget);

            if (filter.isUndefined()) {
                return "Please set at least one option on the filter "
                                + "(other than includeParents, includeSource "
                                + "or includeTarget).";
            }

            StringBuilder sb = new StringBuilder();

            sb.append("Issued cancel command to ").append(
                            poolOperationMap.cancel(filter)).append(
                            " pool operations.");

            if (includeParent || includeSource || includeTarget) {
                sb.append("  Also issued cancel command to ").append(
                                pnfsOperationMap.cancel(filter)).append(
                                " pnfsId operations.");
            }

            return sb.toString();
        }
    }

    abstract class PoolInfoCommand extends ResilienceCommand {
        @Argument(required = false, usage = "Regular expression to match pool "
                        + "names; no argument returns the entire list.")
        String pool;

        @Override
        protected String doCall() throws Exception {
            try {
                return poolInfoMap.getPoolInfo(pool);
            } catch (NoSuchElementException e) {
                return "No such pool: " + pool;
            }
        }
    }

    abstract class PoolOpLsCommand extends ResilienceCommand {
        @Option(name = "poolStatus",
                        valueSpec = "DOWN | RESTART ",
                        usage = "List only operations on pools matching this "
                                        + "pool status.")
        String poolStatus;

        @Option(name = "state",
                        valueSpec = "IDLE | WAITING | RUNNING | FAILED | CANCELED"
                                        + " (default is everything) ",
                        usage = "List only operations on pools matching this "
                                        + "comma-delimited set of operation states.")
        String state = "IDLE,WAITING,RUNNING,FAILED,CANCELED";

        @Option(name = "lastUpdateBefore",
                        valueSpec = FORMAT_STRING,
                        usage = "List only operations on pools whose last "
                                        + "update was before this date-time.")
        String lastUpdateBefore;

        @Option(name = "lastUpdateAfter",
                        valueSpec = FORMAT_STRING,
                        usage = "List only operations on pools whose last "
                                        + "update was after this date-time.")
        String lastUpdateAfter;

        @Option(name = "lastScanBefore",
                        valueSpec = FORMAT_STRING,
                        usage = "List only operations on pools whose scan "
                                        + "update was before this date-time.")
        String lastScanBefore;

        @Option(name = "lastScanAfter",
                        valueSpec = FORMAT_STRING,
                        usage = "List only operations on pools whose scan "
                                        + "update was after this date-time.")
        String lastScanAfter;

        @Argument(required = false,
                        valueSpec = "regular expression",
                        usage = "List only operations on pools matching this "
                                        + "expression.")
        String pools;

        @Override
        protected String doCall() throws Exception {
            PoolFilter filter = new PoolFilter();
            filter.setPool(pools);
            filter.setLastScanAfter(getTimestamp(lastScanAfter));
            filter.setLastScanBefore(getTimestamp(lastScanBefore));
            filter.setLastUpdateBefore(getTimestamp(lastUpdateBefore));
            filter.setLastUpdateAfter(getTimestamp(lastUpdateAfter));
            filter.setPoolStatus(poolStatus);
            filter.setState(state);

            return poolOperationMap.list(filter);
        }
    }

    abstract class PoolNameCommand extends ResilienceCommand {
        @Argument(required = true, usage = "Integer key.")
        String key;

        @Override
        protected String doCall() throws Exception {
            try {
                return poolInfoMap.getPool(Integer.parseInt(key));
            } catch (IndexOutOfBoundsException | NoSuchElementException e) {
                return "No such pool: " + key;
            }
        }
    }

    abstract class ScanCommand extends ResilienceCommand {
        @Argument(required = true,
                        usage = "Regular expression for pool(s) on which to "
                                        + "conduct the adjustment of "
                                        + "all pnfsids.")
        String pools;

        @Override
        protected String doCall() throws Exception {
            Pattern pattern = Pattern.compile(pools);

            StringBuilder builder = new StringBuilder("Scans have been issued for:\n");
            StringBuilder errors = new StringBuilder("ERRORS:\n");

            poolInfoMap.getResilientPools().stream()
                            .filter((pool) -> pattern.matcher(pool).matches())
                            .map((pool) -> poolInfoMap.getPoolState(pool))
                            .forEach((update) -> {
                try {
                    poolOperationMap.scan(update);
                    builder.append("\t").append(update.pool).append("\n");
                } catch (IllegalArgumentException e) {
                    errors.append("\t")
                          .append(String.format("%s, %s", update.pool,
                                                          new ExceptionMessage(e)))
                          .append("\n");
                }
            });

            if (errors.length() > 8) {
                builder.append(errors);
            }

            return builder.toString();
        }
    }

    abstract class UniqueCommand extends ResilienceCommand {
        @Argument(required = true,
                        usage = "Specify a comma-delimited "
                                        + "list of pool names.")
        String pools;

        @Override
        protected String doCall() throws Exception {
            String[] list = pools.split("[,]");
            StringBuilder builder = new StringBuilder();
            Map<String, Integer> results = null;
            String filter = "=1";

            for (String pool : list) {
                pool = pool.trim();
                results = namespaceAccess.getPnfsidCountsFor(pool, filter);
                builder.append(pool).append(":\t")
                                    .append(results.size()).append("\n\n");
                for (String pnfsid : results.keySet()) {
                    builder.append(pnfsid).append("\t")
                                    .append(results.get(pnfsid)).append("\n");
                }
                if (!results.isEmpty()) {
                    builder.append("\n");
                }
            }

            return builder.toString();
        }
    }

    abstract class VerifyCommand extends ResilienceCommand {
        @Argument(index = 0,
                        required = true,
                        usage = "Name of the resilient pool group.")
        String poolGroup;

        @Argument(index = 1,
                        required = false,
                        valueSpec = "ls",
                        usage = "With this argument, "
                                        + "just prints out the storage units.")
        String skip;

        @Override
        protected String doCall() throws Exception {
            Integer index = null;

            try {
                index = poolInfoMap.getGroupIndex(poolGroup);
            } catch (NoSuchElementException e) {
                return  String.format("No such pool group: %s.", poolGroup);
            }

            if (!poolInfoMap.isResilientGroup(index)) {
                return String.format("%s is not a resilient group.", poolGroup);
            }

            if ("ls".equalsIgnoreCase(skip)) {
                return listStorageUnitsForPoolGroup();
            }

            try {
                poolInfoMap.verifyConstraints(index);
            } catch (NoSuchElementException | IllegalStateException e) {
                return String.format("%sAs configured, member pools cannot "
                                                     + "satisfy resilience "
                                                + "constraints: %s\n",
                                listStorageUnitsForPoolGroup(),
                                new ExceptionMessage(e));
            }

            return String.format("%sAs configured, member pools can satisfy "
                                                 + "resilience constraints.",
                                 listStorageUnitsForPoolGroup(),
                                 poolGroup);
        }

        private String listStorageUnitsForPoolGroup() {
            StringBuilder builder = new StringBuilder();
            builder.append("Name         : ").append(poolGroup).append("\n");
            builder.append("Storage Units:\n");

            poolInfoMap.getStorageUnitsFor(poolGroup).stream()
                       .map((i) -> poolInfoMap.getGroupName(i))
                       .forEach((u) -> builder.append("    ")
                                              .append(u).append("\n"));
            return builder.toString();
        }
    }

    private static Long getTimestamp(String datetime) throws ParseException {
        if (datetime == null)
            return null;
        return DATE_FORMAT.parse(datetime).getTime();
    }

    private PoolInfoMap          poolInfoMap;
    private MessageGuard         messageGuard;
    private MapInitializer       initializer;
    private PoolOperationMap     poolOperationMap;
    private PnfsOperationHandler pnfsOperationHandler;
    private PnfsOperationMap     pnfsOperationMap;
    private NamespaceAccess      namespaceAccess;
    private OperationStatistics  counters;
    private OperationHistory     history;

    public void setCounters(OperationStatistics counters) {
        this.counters = counters;
    }

    public void setHistory(OperationHistory history) {
        this.history = history;
    }

    public void setInitializer(MapInitializer initializer) {
        this.initializer = initializer;
    }

    public void setMessageGuard(MessageGuard messageGuard) {
        this.messageGuard = messageGuard;
    }

    public void setNamespaceAccess(NamespaceAccess namespaceAccess) {
        this.namespaceAccess = namespaceAccess;
    }

    public void setPnfsOperationHandler(
                    PnfsOperationHandler pnfsOperationHandler) {
        this.pnfsOperationHandler = pnfsOperationHandler;
    }

    public void setPnfsOperationMap(PnfsOperationMap pnfsOperationMap) {
        this.pnfsOperationMap = pnfsOperationMap;
    }

    public void setPoolInfoMap(PoolInfoMap poolInfoMap) {
        this.poolInfoMap = poolInfoMap;
    }

    public void setPoolOperationMap(PoolOperationMap poolOperationMap) {
        this.poolOperationMap = poolOperationMap;
    }
}
