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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import org.dcache.resilience.data.PnfsOperation;
import org.dcache.resilience.data.PnfsUpdate;
import org.dcache.resilience.data.MessageType;
import org.dcache.resilience.data.PoolInfoMap;
import org.dcache.resilience.handlers.PnfsOperationHandler;
import org.dcache.resilience.util.PoolSelectionUnitDecorator.SelectionAction;
import org.dcache.util.BackgroundForegroundDeque;

/**
 * <p>Static methods for writing and reading data for
 * checkpointing purposes.</p>
 *
 * <p>Experimentation with NIO showed that serialization using ByteBuffer
 *    is not efficient, with large writes (of 1M records or more) taking
 *    on the order of 45 minutes to an hour to complete.</p>
 *
 * <p>This implementation writes out a simple CDL to a text file.</p>
 *
 * Created by arossi on 11/04/2015.
 */
public final class CheckpointUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(CheckpointUtils.class);

    /**
     * <p>Read back in from the checkpoint file operation records.
     *    These are converted to {@link PnfsUpdate} objects and passed
     *    to {@link PnfsOperationHandler#handleBrokenFileLocation(PnfsId, String)}
     *    for registration.</p>
     *
     * @param checkpointFilePath to read
     * @param poolInfoMap for translating names to indices
     * @param handler for registering the updates.
     */
    public static void load(String checkpointFilePath, PoolInfoMap poolInfoMap,
                    PnfsOperationHandler handler) {
        if (!new File(checkpointFilePath).exists()) {
            return;
        }

        try (BufferedReader fr = new BufferedReader(new FileReader(checkpointFilePath))) {
            while (true) {
                String line = fr.readLine();
                if (line == null) {
                    break;
                }
                try {
                    PnfsUpdate update = fromString(line, poolInfoMap);
                    if (update != null) {
                        handler.handleLocationUpdate(update);
                    }
                } catch (CacheException e) {
                    LOGGER.debug("Unable to reload operation for {}; {}",
                                    line, e.getMessage());
                }
            }
        } catch (FileNotFoundException e) {
            LOGGER.error("Unable to reload checkpoint file: {}", e.getMessage());
        } catch (IOException e) {
            LOGGER.error("Unrecoverable error during reload checkpoint file: {}",
                            e.getMessage());
        }
    }

    /**
     * <p>Since we use checkpointing as an approximation,
     * the fact that the ConcurrentMap (internal to the deque class)
     * may be dirty and that it cannot be locked should not matter.</p>
     *
     * @param checkpointFilePath where to write.
     * @param poolInfoMap for translation of indices to names.
     * @param deque contains a ConcurrentHashMap implementation of its index.
     * @return number of records written
     */
    public static long save(String checkpointFilePath, PoolInfoMap poolInfoMap,
                    BackgroundForegroundDeque<PnfsId, PnfsOperation> deque) {
        File current = new File(checkpointFilePath);
        File old = new File(checkpointFilePath + "-old");
        if (current.exists()) {
            current.renameTo(old);
        }

        AtomicLong count = new AtomicLong(0);
        Iterator<PnfsOperation> iterator = deque.getValueIterator();
        StringBuilder builder = new StringBuilder();

        try (PrintWriter fw = new PrintWriter(new FileWriter(checkpointFilePath, false))) {
            while (iterator.hasNext()) {
                PnfsOperation operation = iterator.next();
                if (toString(operation, builder, poolInfoMap)) {
                    fw.println(builder.toString());
                    count.incrementAndGet();
                    builder.setLength(0);
                    if (count.get() % 1000 == 0) {
                        fw.flush();
                    }
                }
            }
            fw.flush();
        } catch (FileNotFoundException e) {
            LOGGER.error("Unable to save checkpoint file: {}", e.getMessage());
        } catch (IOException e) {
            LOGGER.error("Unrecoverable error during save of checkpoint file: {}",
                            e.getMessage());
        }

        return count.get();
    }

    /**
     *  <p>Write out the operation's relevant fields to the buffer.</p>
     */
    static boolean toString(PnfsOperation operation, StringBuilder builder,
                        PoolInfoMap map) {
        Integer parent = operation.getParent();
        Integer source = operation.getSource();
        String pool = parent == null ?
                        (source == null ? null: map.getPool(source)):
                        map.getPool(parent);
        if (pool == null) {
            /*
             *  Incomplete record.  Skip.
             */
            return false;
        }

        builder.append(operation.getPnfsId()).append(",");
        builder.append(operation.getSelectionAction()).append(",");
        builder.append(parent != null).append(",");
        builder.append(operation.getOpCount()).append(",");
        builder.append(map.getGroup(operation.getPoolGroup())).append(",");
        builder.append(pool);

        return true;
    }

    /**
     * @return update object constructed from the parsed line.
     */
    static PnfsUpdate fromString(String line, PoolInfoMap map) {
        String[] parts = line.split("[,]");
        if (parts.length != 6) {
            return null;
        }
        PnfsId pnfsId = new PnfsId(parts[0]);
        SelectionAction action = SelectionAction.values()[Integer.parseInt(parts[1])];
        boolean isParent = Boolean.valueOf(parts[2]);
        int opCount = Integer.parseInt(parts[3]);
        Integer gindex = map.getGroupIndex(parts[4]);
        PnfsUpdate update = new PnfsUpdate(pnfsId, parts[5],
                        isParent ? MessageType.POOL_STATUS_RESTART :
                                   MessageType.NEW_FILE_LOCATION,
                        action, gindex);
        update.setCount(opCount);
        update.setFromReload(true);
        return update;
    }

    private CheckpointUtils() {}
}
