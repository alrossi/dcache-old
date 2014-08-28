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
package org.dcache.replication.admin;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import java.io.FileWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionLink;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPool;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPoolGroup;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionUnit;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionUnitGroup;
import diskCacheV111.poolManager.StorageUnit;
import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.Pool2PoolTransferMsg;

import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.nucleus.SerializationException;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import dmg.util.command.Option;

import org.dcache.cells.CellStub;
import org.dcache.replication.api.PnfsCacheMessageType;
import org.dcache.replication.api.ReplicationEndpoints;
import org.dcache.replication.api.ReplicationMessageReceiver;
import org.dcache.replication.api.ReplicationOperationRegistry;
import org.dcache.replication.api.ReplicationQueryUtilities;
import org.dcache.replication.api.ReplicationScanType;
import org.dcache.replication.api.ReplicationStatistics;
import org.dcache.replication.api.ReplicationTaskExecutor;
import org.dcache.replication.data.PnfsIdMetadata;
import org.dcache.replication.data.PoolGroupMetadata;
import org.dcache.replication.data.PoolMetadata;
import org.dcache.replication.runnable.PeriodicScanner;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.replication.ListPnfsidsForPoolMessage;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Control procedures for the admin shell.
 *
 * @author arossi
 */
public final class AdminCommandHandler implements CellCommandListener {

    enum AdjustMode {
        FILE, POOL, POOLGROUP;

        public static AdjustMode parseOption(String option) {
            switch(option.toUpperCase()) {
                case "FILE":
                    return AdjustMode.FILE;
                case "GROUP":
                    return AdjustMode.POOLGROUP;
                case "POOL":
                    return AdjustMode.POOL;
                default:
                    throw new IllegalArgumentException("Unrecognized "
                                    + "option: '" + option + "'.");
            }
        }
    }

    enum ScanMode {
        ON, OFF, RUN, INFO;

        public static ScanMode parseOption(String option) {
            switch(option.toUpperCase()) {
                case "ON":
                    return ScanMode.ON;
                case "OFF":
                    return ScanMode.OFF;
                case "RUN":
                    return ScanMode.RUN;
                case "INFO":
                    return ScanMode.INFO;
                default:
                    throw new IllegalArgumentException("Unrecognized "
                                    + "option: '" + option + "'.");
            }
        }
    }

    enum TaskMode {
        CLEAR, INFO;

        public static TaskMode parseOption(String option) {
            switch(option.toUpperCase()) {
                case "CLEAR":
                    return TaskMode.CLEAR;
                case "INFO":
                    return TaskMode.INFO;
                default:
                    throw new IllegalArgumentException("Unrecognized "
                                    + "option: '" + option + "'.");
            }
        }
    }

    static class LsOpts {
        private static final byte GROUP = 0x1;
        private static final byte ACTIVE = 0x2;
        private static final byte COUNT = 0x4;

        private byte value = 0x0;

        boolean isActive() {
            return (value & ACTIVE) == ACTIVE;
        }

        boolean isCount() {
            return (value & COUNT) == COUNT;
        }

        boolean isGroup() {
            return (value & GROUP) == GROUP;
        }

        void parse(char[] options) {
            for (char c: options) {
                switch(c) {
                    case 'g':
                        value |= GROUP;
                        break;
                    case 'a':
                        value |= ACTIVE;
                        break;
                    case 'c':
                        value |= COUNT;
                        break;
                    default:
                        throw new IllegalArgumentException("Unrecognized "
                                        + "option: '" + c + "'.");
                }
            }
        }
    }

    @Command(name = "adjust",
             hint = "Adjust the number of replicas for a single file, a pool"
                             + " or a pool group.",
             description = "Verifies (asynchronously) the actual number of"
                             + " (accessible) copies and checks it against"
                             + " the pertinent constraints, issuing a request"
                             + " for replication or reduction if necessary.")
    class AdjustCommand implements Callable<String> {
        @Argument(index = 0,
                  valueSpec = "file | pool | group ",
                  required = true,
                  usage = "One of file, pool, or group, indicating the"
                                  + " type of the second argument.")
        String option;

        @Argument(index = 1,
                  required = true,
                  usage = "Pnfsid of file, or name of pool or pool group.")
        String target;

        private AdjustMode type = AdjustMode.POOL;

        @Override
        public String call() throws Exception {
            type = AdjustMode.parseOption(option);

            PoolSelectionUnit psu = hub.getPoolMonitor().getPoolSelectionUnit();

            switch(type) {
                case FILE:
                    PnfsId pnfsId = new PnfsId(target);
                    List<String> pools = utils.getCacheLocations(pnfsId);
                    String source = utils.removeRandomEntry(pools);
                    if (source == null) {
                        return target + " is inaccessible "
                                        + "and in need of manual recovery.";
                    }
                    PnfsIdMetadata opData = new PnfsIdMetadata(pnfsId,
                                                               source,
                                                               PnfsCacheMessageType.SCAN,
                                                               psu,
                                                               utils);
                    map.register(opData);
                    executor.submitVerifyTask(opData);
                    break;
                case POOLGROUP:
                    Map<String, SelectionPoolGroup> groups = psu.getPoolGroups();
                    SelectionPoolGroup group = groups.get(target);
                    if (group == null) {
                        return target + " is not a recognized pool group.";
                    }
                    if (group.getMinReplicas() == 1) {
                        return target + " is not resilient.";
                    }
                    String pgName = group.getName();
                    Collection<SelectionPool> pgPools
                        = psu.getPoolsByPoolGroup(pgName);
                    for (SelectionPool pool: pgPools) {
                        PoolMetadata poolData = new PoolMetadata(pool.getName(),
                                                                 psu,
                                                                 utils);
                        executor.submitScanFullTask(poolData);
                    }
                    break;
                case POOL:
                    PoolMetadata poolData = new PoolMetadata(target, psu, utils);
                    if (poolData.poolGroupData.poolGroup == null){
                        return target + " does not belong to a resilient"
                                      + " pool group.";
                    }
                    executor.submitScanFullTask(poolData);
                    break;
            }

            return "Running 'adjust " + option + " " + target + "'";
        }
    }

    @Command(name = "drainoff",
             hint = "Copy all unique files from the source pool to other pools"
                             + " in the source's replicating group, or to the"
                             + " indicated pool.",
             description = "Requests a listing for the unique files and issues"
                             + " a p2p request to the selected pool.  If the"
                             + " pool is not explicitly chosen, it is randomly"
                             + " selected from the pools in the pool group.")
    class DrainoffCommand implements Callable<String> {
        @Argument(index = 0,
                  required = true,
                  usage = "Name of pool to drain.")
        String source;

        @Argument(index = 1,
                  required = false,
                  usage = "Name of target pool.")
        String target;

        private PoolMetadata data;

        @Override
        public String call() throws Exception {
            PoolSelectionUnit psu = hub.getPoolMonitor().getPoolSelectionUnit();
            data = new PoolMetadata(source, psu, utils);

            if (data.poolGroupData.poolGroup == null) {
                return source + " does not belong to a replicating pool group";
            }

            ListPnfsidsForPoolMessage message
                = utils.getListMessage(data, ReplicationScanType.UNIQUE, true);
            Multimap<String, String> replicas = list(message,
                                                     hub.getPnfsManager());

            for(String pnfsid: replicas.asMap().keySet()) {
                String target = getTarget();
                if (target != null) {
                    requestP2p(pnfsid, target);
                } else {
                    throw new IllegalStateException(pnfsid + " is inaccessible;"
                                    + " interrupting drainoff.");
                }
            }

            String actualTarget = target == null ?
                                  data.poolGroupData.poolGroup.getName() :
                                  target;

            return "Running 'drainoff of " + source
                                           + " to "
                                           + actualTarget
                                           + "'.";
        }

        private String getTarget() {
            if (target != null) {
                return target;
            }

            List<String> pools
                = utils.getActivePoolsInGroup(data.poolGroupData.poolGroup,
                                              data.poolGroupData.psu);
            pools.remove(source);
            return utils.removeRandomEntry(pools);
        }

        private void requestP2p(String pnfsid, String target)
                        throws SerializationException, NoRouteToCellException {
            PnfsId pnfsId = new PnfsId(pnfsid);
            FileAttributes fileAttributes = new FileAttributes();
            fileAttributes.setPnfsId(pnfsId);
            fileAttributes.setAccessLatency(AccessLatency.ONLINE);
            Pool2PoolTransferMsg p2p =
                 new Pool2PoolTransferMsg(source, target, fileAttributes);
            p2p.setDestinationFileStatus(Pool2PoolTransferMsg.CACHED);
            hub.getEndpoint().sendMessage(new CellMessage(new CellPath(target),
                                                          p2p));
        }
    }

    @Command(name = "ls",
             hint = "List pnfsids and locations or counts, according "
                               + "to specified criteria.",
             description = "Issues a query to the namespace to find qualifying "
                               + " replicas as specified by options; results"
                               + " can be written to a file. Note that a file"
                               + " is listed here according to the pool constraints,"
                               + " not any ulterior storage constraints; thus"
                               + " extra or deficient counts may not reflect"
                               + " the actual requirements for the individual"
                               + " pnfsid.")
    class ListCommand implements Callable<String> {
        @Argument(index = 0,
                  valueSpec = "u|r|d[a][g][c] ",
                  required = true,
                  usage = "One of u[nique], r[edundant], d[eficient]; optionally"
                                  + " a[ctiveOnly], g[roup, indicating a"
                                  + " listing for the entire resilient group], or"
                                  + " c[count, to show replica count in lieu of"
                                  + " actual locations], the last"
                                  + " three appended to the first letter"
                                  + " (in any order); e.g."
                                  + " 'uag', dgac, etc.")
        String options;

        @Option(name = "o",
                usage = "An output file path to which to write the result.")
        String output;

        @Argument(index = 1,
                  required = true,
                  usage = "Specify the pool or pool-group (with option g) name.")
        String name;

        private ReplicationScanType type;
        private final LsOpts opts = new LsOpts();

        @Override
        public String call() throws Exception {
            try {
                type = ReplicationScanType.parseOption(options.charAt(0));

                if (options.length() > 1) {
                    opts.parse(options.substring(1).toCharArray());
                }

                Multimap<String, String> replicas = getReplicaMap();

                if (replicas == null) {
                    return name + " does not belong to a resilient pool group.";
                }

                String listing = toString(replicas);

                if (output != null) {
                    FileWriter fw = new FileWriter(output);
                    try {
                        fw.write(listing);
                        fw.flush();
                    } finally {
                        fw.close();
                    }
                }

                return listing;
            } catch (Exception e) {
                e.printStackTrace();
                throw e;
            }
          }

        private Multimap<String, String> getReplicaMap()
                        throws CacheException,
                               InterruptedException,
                               TimeoutException {
            PoolSelectionUnit psu = hub.getPoolMonitor().getPoolSelectionUnit();
            if (opts.isGroup()) {
                Collection<SelectionPool> pools = psu.getPoolsByPoolGroup(name);
                if (pools.isEmpty()) {
                    throw new CacheException("Something is wrong: "
                                             + "there don't seem to be any pools"
                                             + " in " + name );
                }
                name = pools.iterator().next().getName();
            }

            PoolMetadata data = new PoolMetadata(name, psu, utils);

            if (data.poolGroupData.poolGroup == null) {
                return null;
            }

            Multimap<String, String> replicas;
            if (opts.isGroup()) {
                List<ListPnfsidsForPoolMessage> messages =
                                getListMessages(data.poolGroupData,
                                                type,
                                                opts.isActive());
                replicas = list(messages, hub.getPnfsManager());
            } else {
                ListPnfsidsForPoolMessage message =
                                utils.getListMessage(data,
                                                     type,
                                                     opts.isActive());
                replicas = list(message, hub.getPnfsManager());
            }

            return replicas;
        }

        private String toString(Multimap<String, String> replicas) {
            StringBuilder builder = new StringBuilder();
            if (!replicas.isEmpty()) {
                if (!opts.isCount()) {
                    for (Entry<String, String> entry : replicas.entries()) {
                        builder.append(entry.getKey())
                               .append("\t")
                               .append(entry.getValue())
                               .append("\n");
                    }
                } else {
                    Iterator<Entry<String, String>> i = replicas.entries()
                                                                .iterator();
                    Entry<String, String> last = i.next();
                    Entry<String,String> next = null;
                    int count = 1;

                    while(i.hasNext()) {
                        next = i.next();
                        if (next.getKey().equals(last.getKey())) {
                            ++count;
                        } else {
                            builder.append(last.getKey())
                                   .append("\t")
                                   .append(count)
                                   .append("\n");
                            last = next;
                            next = null;
                            count = 1;
                        }
                    }

                    if (next != null) {
                        builder.append(last.getKey())
                               .append("\t")
                               .append(count)
                               .append("\n");
                    }
                }
            } else {
                builder.append("No matches.");
            }

            return builder.toString();
        }
    }

    @Command(name = "minmaxof",
             hint = "List replica min and max values for the pool group.",
             description = "Gives the default values for the pool, a list"
                             + " of related storage groups with their values,"
                             + " and the adjusted value for the pool"
                             + " (computed as the greatest lower bound and least"
                             + " upper bound from the storage group members). The"
                             + " latter is used to list all possible deficient"
                             + " and redundant files, and will produce false"
                             + " positives but no false negatives.")
    class MinMaxOfCommand implements Callable<String> {
        @Argument(index = 0,
                  required = true,
                  usage = "Name of pool group.")
        String name;

        public String call() throws Exception {
            PoolSelectionUnit psu =hub.getPoolMonitor().getPoolSelectionUnit();
            SelectionPoolGroup poolGroup = psu.getPoolGroups().get(name);
            if (poolGroup == null) {
                return "No such pool group: " + name + ".";
            }

            int minimum = poolGroup.getMinReplicas();

            if (minimum < 2) {
                return "Pool group " + name + " is not resilient.";
            }

            int maximum = poolGroup.getMaxReplicas();

            StringBuilder sb = new StringBuilder();
            sb.append("Default  mininum: \t\t").append(minimum).append("\n");
            sb.append("Default  maximum: \t\t").append(maximum).append("\n\n");

            Collection<SelectionLink> links
                = psu.getLinksPointingToPoolGroup(poolGroup.getName());
            for (SelectionLink link : links) {
                Collection<SelectionUnitGroup> ugroups
                    = link.getUnitGroupsTargetedBy();
                for (SelectionUnitGroup ugroup : ugroups) {
                    Collection<SelectionUnit> units = ugroup.getMemeberUnits();
                    for (SelectionUnit unit : units) {
                        if (unit instanceof StorageUnit) {
                            StorageUnit sunit = (StorageUnit) unit;
                            String suName = sunit.getCanonicalName();
                            Integer smax = sunit.getMaxReplicas();
                            Integer smin = sunit.getMinReplicas();
                            if (smax != null) { // both should be valid
                                maximum = Math.min(maximum, smax);
                                minimum = Math.max(minimum, smin);
                            }
                            sb.append("\t")
                              .append(suName)
                              .append("\t")
                              .append("\t")
                              .append("(")
                              .append(smin)
                              .append(", ")
                              .append(smax)
                              .append(")")
                              .append("\n");
                        }
                    }
                }
            }

            sb.append("\nGreatest mininum: \t\t").append(minimum).append("\n");
            sb.append("Least    maximum: \t\t").append(maximum).append("\n");
            sb.append("Use greedy requests: \t\t")
              .append(utils.isUseGreedyRequests()).append("\n");

            return sb.toString();
        }
    }

    @Command(name = "scan",
             hint = "Control the periodic scan of active resilient pools.",
             description = "Activate, turn off, or reschedule the periodic check "
                             + "of active pools for deficient and excessive copies "
                             + "which issues the appropriate requests")
    class ScanCommand implements Callable<String> {
        @Argument(index = 0,
                  valueSpec = "off|on|info|run ",
                  required = true,
                  usage = "off = turn the scanner off; on = turn the scanner on; "
                                  + "info = show when the next scan is scheduled "
                                  + "to run; run = override the currently scheduled "
                                  + "scan to run when indicated (default means now); "
                                  + "automatic periodic checking resumes afterwards, "
                                  + "with a new next time being scheduled using the "
                                  + "default timeout period.")
        String operation;

        @Argument(index = 1, required = false,
                  usage = "Specify when to run the scan using "
                                  + "the format 'yyyy/MM/dd HH:mm:ss'.")
        String next;

        @Override
        public String call() throws Exception {
            ScanMode mode = ScanMode.valueOf(operation.toUpperCase());
            switch(mode) {
                case INFO:
                    if (!scanner.isRunning()) {
                        return "Scanner is off.";
                    }
                    return "Next scan: " + new Date(scanner.getNextScan());
                case OFF:
                    if (scanner.isRunning()) {
                        scanner.shutdown();
                        return "Shut down scanner.";
                    }
                    return "Scanner already off.";
                case ON:
                    if (!scanner.isRunning()) {
                        long time = System.currentTimeMillis()
                                        + scanner.getTimeoutUnit()
                                                 .toMillis(scanner.getTimeout());
                        scanner.reschedule(time, TimeUnit.MILLISECONDS);
                        scanner.initialize();
                        return "Scanner is on, next scan scheduled for "
                            + new Date(time).toString() + ".";
                    }
                    return "Scanner already on.";
                case RUN:
                    if (!scanner.isRunning()) {
                        return "Scanner is off; turn scan on first.";
                    }

                    long time;
                    if (next == null) {
                        time = System.currentTimeMillis() - 1000;
                    } else {
                        time = DATE_FORMAT.parse(next).getTime();
                    }

                    scanner.reschedule(time, TimeUnit.MILLISECONDS);
                    return "Scan rescheduled for "
                        + new Date(time).toString() + ".";
            }

            return "Next scan scheduled for "
                            + new Date(scanner.getNextScan()).toString()  + ".";
        }
    }

    @Command(name = "set greedy",
                    hint = "Change logic governing the number of copies to "
                                    + "request for replication or removal.",
                    description = "True means that when a replication request "
                                    + "is issued, the highest reasonable number "
                                    + "of replicas will be requested (the "
                                    + "maximum minus the number currently "
                                    + "accessible on UP pools in the group), "
                                    + "and when a reduction request is issued, "
                                    + "all but the minimum number will be eliminated. "
                                    + "When set to false, the replication request "
                                    + "will ask for the minimum and the reduction "
                                    + "request will eliminate all down to the "
                                    + "maximum number of copies.")
    class SetCommand implements Callable<String> {
        @Argument(index = 0,
                  valueSpec = "true | false ",
                  required = true)
        String value;

        @Override
        public String call() throws Exception {
            boolean previous = utils.isUseGreedyRequests();
            boolean changedto = Boolean.valueOf(value);
            utils.setUseGreedyRequests(changedto);
            return "replicamanager.requests.use-greedy-limits changed from "
                + previous + " to " + changedto;
        }
    }

    @Command(name = "stat",
             hint = "Show statistics: counts for messages and tasks.",
             description = "Accesses internal counters.")
    class StatCommand implements Callable<String> {
        @Override
        public String call() throws Exception {
            return statistics.print();
        }
    }

    @Command(name = "tasks",
             hint = "Clear current task tokens or list current tokens.",
             description = "Removes entries from the internal operations "
                             + "map and/or prints out map.")
    class TasksCommand implements Callable<String> {
        @Argument(index = 0,
                  valueSpec = "clear|info ",
                        required = true,
                        usage = "Either clear the operation token(s) from the "
                                        + "map or display the contents of the map.")
        String option;

        @Argument(index = 1,
                  required = false,
                  usage = "An regular expression to use to match the token;"
                                        + " if no expression is given, the"
                                        + " entire map is cleared (this is"
                                        + " inherently dangerous and should be"
                                        + " done with caution); else elements"
                                        + " will be removed whose group+pnfsid "
                                        + " match the expression.")
        String expression;

        @Override
        public String call() throws Exception {
            TaskMode type = TaskMode.parseOption(option);

            switch(type) {
                case CLEAR:
                    if (expression != null) {
                        map.removeMatchingKeys(expression);
                    } else {
                        map.clear();
                    }
                    break;
                default:
                      /*
                       * Just print the map.
                       */

            }

            return map.print();
        }
    }

    private static final DateFormat DATE_FORMAT
        = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

    static Multimap<String, String> list(List<ListPnfsidsForPoolMessage> messages,
                                         CellStub pnfsManager)
                    throws CacheException,
                           InterruptedException,
                           TimeoutException {
        Multimap<String, String> all = ArrayListMultimap.create();
        for (ListPnfsidsForPoolMessage message: messages) {
            Multimap<String, String> submap = list(message, pnfsManager);
            for (Entry<String, String> entry: submap.entries()) {
                String key = entry.getKey();
                String value = entry.getValue();
                if (!all.containsEntry(key, value)) {
                    all.put(key, value);
                }
            }
        }
        return all;
    }

    static Multimap<String, String> list(ListPnfsidsForPoolMessage message,
                                         CellStub pnfsManager)
                    throws CacheException,
                           InterruptedException,
                           TimeoutException {
        message = pnfsManager.sendAndWait(message);
        if (message == null) {
            throw new TimeoutException("Request for list timed out.");
        }
        return message.getReplicas();
    }

    private ReplicationEndpoints hub;
    private ReplicationTaskExecutor executor;
    private ReplicationMessageReceiver handler;
    private ReplicationOperationRegistry map;
    private ReplicationQueryUtilities utils;
    private PeriodicScanner scanner;
    private ReplicationStatistics statistics;

    public void initialize() {
        checkNotNull(hub);
        checkNotNull(executor);
        checkNotNull(handler);
        checkNotNull(map);
        checkNotNull(utils);
        checkNotNull(scanner);
        checkNotNull(statistics);
    }

    public void setExecutor(ReplicationTaskExecutor executor) {
        this.executor = executor;
    }

    public void setHandler(ReplicationMessageReceiver handler) {
        this.handler = handler;
    }

    public void setHub(ReplicationEndpoints hub) {
        this.hub = hub;
    }

    public void setMap(ReplicationOperationRegistry map) {
        this.map = map;
    }

    public void setScanner(PeriodicScanner scanner) {
        this.scanner = scanner;
    }

    public void setStatistics(ReplicationStatistics statistics) {
        this.statistics = statistics;
    }

    public void setUtils(ReplicationQueryUtilities utils) {
        this.utils = utils;
    }

    @SuppressWarnings("null")
    List<ListPnfsidsForPoolMessage> getListMessages(PoolGroupMetadata data,
                                                    ReplicationScanType type,
                                                    boolean activeOnly)
                    throws CacheException,
                           InterruptedException,
                           TimeoutException {
        data.constraints.verifyConstraintsForPoolGroup(data.poolGroup, data.psu);
        int minimum = data.constraints.getMinimum();
        int maximum = data.constraints.getMaximum();

        List<String> pools = new ArrayList<>();
        List<String> active = null;
        List<String> inactive = null;

        if (activeOnly) {
            active = new ArrayList();
            inactive = new ArrayList();
        }

        utils.getPoolsInGroup(data.poolGroup, data.psu, pools, active, inactive);

        if (activeOnly) {
            pools = active;
        }

        List<ListPnfsidsForPoolMessage> messages = new ArrayList<>();

        for (String pool: pools) {
            messages.add(utils.getListMessage(pool,
                                              type,
                                              minimum,
                                              maximum,
                                              inactive));
        }

        return messages;
    }
}
