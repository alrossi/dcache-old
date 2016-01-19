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

import diskCacheV111.pools.PoolV2Mode;
import diskCacheV111.vehicles.PoolStatusChangedMessage;
import org.dcache.resilience.data.MessageType;

/**
 * <p> Interprets a {@link PoolStatusChangedMessage}
 * for potential resilience handling by examining both the state
 * and the pool mode transmitted by the message.  Also provides a
 * transition table for determining the next status when successive
 * such messages are received. </p>
 *
 * Created by arossi on 1/26/15.
 */
public enum PoolStatusForResilience {
    DOWN,               // from DOWN pool status
    RESTART,            // from RESTART or UP pool status, depending on mode
    UP,                 // from RESTART or UP pool status, depending on mode
    DOWN_IGNORE,        // from DOWN on pool with resilience suppressed
    UP_IGNORE,          // from UP/RESTART on pool with resilience suppressed
    CANCEL,             // cancel any running operation
    NOP;                // leave any running or waiting operation in place

    final static int[] notReadable = { PoolV2Mode.DISABLED_DEAD,
                                       PoolV2Mode.DISABLED_STRICT,
                                       PoolV2Mode.DISABLED_P2P_SERVER,
                                       PoolV2Mode.DISABLED_FETCH };

    final static int[] readable = { PoolV2Mode.DISABLED_P2P_CLIENT,
                                    PoolV2Mode.DISABLED_RDONLY,
                                    PoolV2Mode.DISABLED_STAGE,
                                    PoolV2Mode.DISABLED_STORE };

    /**
     * This is the value used to determine what to do next.
     */
    public static PoolStatusForResilience getStatusFor(
                    PoolStatusChangedMessage message) {
        return getStatusFor(message.getPoolState(), message.getPoolMode());
    }

    public static PoolStatusForResilience getStatusFor(int poolState,
                                                       PoolV2Mode poolMode) {
        PoolStatusForResilience state = UP;

        switch (poolState) {
            case PoolStatusChangedMessage.DOWN:
                state = DOWN;
                break;
            case PoolStatusChangedMessage.RESTART:
                state = RESTART;
            default:
        }

        if (poolMode == null) {
            return state;
        }

        int mode = poolMode.getMode();

        /*
         *  As a precaution, UP/DOWN are normalized explicitly
         *  according to whether the pool is readable and/or writable.
         */
        for (int mask : readable) {
            if ((mode & mask) == mask) {
                if (state == DOWN) {
                    state = UP;
                    break;
                }
            }
        }

        for (int mask : notReadable) {
            if ((mode & mask) == mask) {
                state = DOWN;
                break;
            }
        }

        if (!poolMode.isReplicationEnabled()) {
            if (state == DOWN) {
                state = DOWN_IGNORE;
            } else {
                state = UP_IGNORE;
            }
        }

        return state;
    }

    public MessageType getMessageType() {
        switch (this) {
            case DOWN:
                return MessageType.POOL_STATUS_DOWN;
            default:
                return MessageType.POOL_STATUS_RESTART;
        }
    }

    /**
     * <p>
     * The implicit transition table is as follows:
     * </p>
     * <table>
     * <tr>
     * <th><i>(This)/Incoming = </i></th>
     * <th>DOWN</th>
     * <th>UP</th>
     * <th>RESTART</th>
     * <th>*-IGNORE</th>
     * </tr>
     * <tr>
     * <td>DOWN</td>
     * <td>NOP</td>
     * <td>CANCEL</td>
     * <td>RESTART</td>
     * <td>NOP</td>
     * </tr>
     * <tr>
     * <td>RESTART</td>
     * <td>DOWN</td>
     * <td>NOP</td>
     * <td>NOP</td>
     * <td>NOP</td>
     * </tr>
     * </table>
     * <p>
     */
    public PoolStatusForResilience getNext(PoolStatusForResilience incoming) {
        switch (this) {
            case DOWN:
                switch (incoming) {
                    case DOWN:
                        return PoolStatusForResilience.NOP;
                    case UP:
                        return PoolStatusForResilience.CANCEL;
                    case RESTART:
                        return PoolStatusForResilience.RESTART;
                    case DOWN_IGNORE:
                        return PoolStatusForResilience.NOP;
                    case UP_IGNORE:
                        return PoolStatusForResilience.NOP;
                    default:
                        return PoolStatusForResilience.NOP;
                }
            case RESTART:
                switch (incoming) {
                    case DOWN:
                        return PoolStatusForResilience.DOWN;
                    case UP:
                        return PoolStatusForResilience.NOP;
                    case RESTART:
                        return PoolStatusForResilience.NOP;
                    case DOWN_IGNORE:
                        return PoolStatusForResilience.NOP;
                    case UP_IGNORE:
                        return PoolStatusForResilience.NOP;
                    default:
                        return PoolStatusForResilience.NOP;
                }
            case UP:
            case UP_IGNORE:
            case DOWN_IGNORE:
            default:
                /*
                 * UP and IGNORE messages get passed along, but
                 * should not trigger any <i>new</i> action on the part
                 * of the message handler.  So while these states are possible
                 * on <b>incoming</b>, they should not have been stored, and
                 * hence should not be possible for <b>this</b>.
                 */
                return PoolStatusForResilience.NOP;
        }
    }

    /**
     * <p>The conversion from the pool state+mode to PoolStatusForReplication
     * restricts 'UP' to meaning the pool is readable
     * (UP with mode strict becomes DOWN here).</p>
     *
     * <p>UP under this interpretation is not actionable,
     * and must be accompanied by an actual RESTART condition
     * for a task to be submitted.  DOWN, of course, will always
     * cause a task to be submitted.</p>
     */
    public boolean isValidForAction() {
        switch (this) {
            case DOWN:
            case RESTART:
                return true;
            default:
                return false;

        }
    }
}
