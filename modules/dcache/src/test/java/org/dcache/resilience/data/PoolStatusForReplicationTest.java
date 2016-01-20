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

import org.junit.Test;

import diskCacheV111.pools.PoolV2Mode;
import diskCacheV111.vehicles.PoolStatusChangedMessage;

import static org.junit.Assert.assertEquals;

/**
 * Created by arossi on 9/21/15.
 */
public final class PoolStatusForReplicationTest {

    PoolStatusChangedMessage message;
    PoolStatusForResilience last;
    PoolStatusForResilience current;
    PoolStatusForResilience next;

    boolean replicationDisabled = false;

    @Test
    public void statusShouldBeCancelForDownFollowedByUp() {
        givenPoolStatusDownMessageWithMode(PoolV2Mode.DISABLED_STRICT);
        givenPoolStatusUpMessageWithMode(PoolV2Mode.DISABLED_RDONLY);
        assertEquals(next, PoolStatusForResilience.CANCEL);
    }

    @Test
    public void statusShouldBeDownForRestartFollowedByDown() {
        givenPoolStatusRestartMessageWithMode(PoolV2Mode.ENABLED);
        givenPoolStatusDownMessageWithMode(PoolV2Mode.DISABLED_STRICT);
        assertEquals(next, PoolStatusForResilience.DOWN);
    }

    @Test
    public void statusShouldBeDownForUpMessageButUnreadable() {
        givenPoolStatusUpMessageWithMode(PoolV2Mode.DISABLED_STRICT);
        assertEquals(current, PoolStatusForResilience.DOWN);
    }

    @Test
    public void statusShouldBeDownIgnoreForDownButReplicationDisabled() {
        givenReplicationIsDisabled();
        givenPoolStatusDownMessageWithMode(PoolV2Mode.DISABLED_STRICT);
        assertEquals(current, PoolStatusForResilience.DOWN_IGNORE);
    }

    @Test
    public void statusShouldBeNOPForDownFollowedByDown() {
        givenPoolStatusDownMessageWithMode(PoolV2Mode.DISABLED_STRICT);
        givenPoolStatusDownMessageWithMode(PoolV2Mode.DISABLED_STRICT);
        assertEquals(next, PoolStatusForResilience.NOP);
    }

    @Test
    public void statusShouldBeNOPForDownFollowedByDownIgnore() {
        givenPoolStatusDownMessageWithMode(PoolV2Mode.DISABLED_STRICT);
        givenPoolStatus(PoolStatusForResilience.DOWN_IGNORE);
        assertEquals(next, PoolStatusForResilience.NOP);
    }

    @Test
    public void statusShouldBeNOPForDownFollowedByUpIgnore() {
        givenPoolStatusDownMessageWithMode(PoolV2Mode.DISABLED_STRICT);
        givenPoolStatus(PoolStatusForResilience.UP_IGNORE);
        assertEquals(next, PoolStatusForResilience.NOP);
    }

    @Test
    public void statusShouldBeNOPForRestartFollowedByDownIgnore() {
        givenPoolStatusRestartMessageWithMode(PoolV2Mode.ENABLED);
        givenPoolStatus(PoolStatusForResilience.DOWN_IGNORE);
        assertEquals(next, PoolStatusForResilience.NOP);
    }

    @Test
    public void statusShouldBeNOPForRestartFollowedByRestart() {
        givenPoolStatusRestartMessageWithMode(PoolV2Mode.ENABLED);
        givenPoolStatusRestartMessageWithMode(PoolV2Mode.ENABLED);
        assertEquals(next, PoolStatusForResilience.NOP);
    }

    @Test
    public void statusShouldBeNOPForRestartFollowedByUp() {
        givenPoolStatusRestartMessageWithMode(PoolV2Mode.ENABLED);
        givenPoolStatusUpMessageWithMode(PoolV2Mode.DISABLED_RDONLY);
        assertEquals(next, PoolStatusForResilience.NOP);
    }

    @Test
    public void statusShouldBeNOPForRestartFollowedByUpIgnore() {
        givenPoolStatusRestartMessageWithMode(PoolV2Mode.ENABLED);
        givenPoolStatus(PoolStatusForResilience.UP_IGNORE);
        assertEquals(next, PoolStatusForResilience.NOP);
    }

    @Test
    public void statusShouldBeRestartForDownFollowedByRestart() {
        givenPoolStatusDownMessageWithMode(PoolV2Mode.DISABLED_STRICT);
        givenPoolStatusRestartMessageWithMode(PoolV2Mode.ENABLED);
        assertEquals(next, PoolStatusForResilience.RESTART);
    }

    @Test
    public void statusShouldBeUpIgnoreForRestartButReplicationDisabled() {
        givenReplicationIsDisabled();
        givenPoolStatusRestartMessageWithMode(PoolV2Mode.ENABLED);
        assertEquals(current, PoolStatusForResilience.UP_IGNORE);
    }

    @Test
    public void statusShouldBeUpIgnoreForUpButReplicationDisabled() {
        givenReplicationIsDisabled();
        givenPoolStatusUpMessageWithMode(PoolV2Mode.DISABLED_RDONLY);
        assertEquals(current, PoolStatusForResilience.UP_IGNORE);
    }

    private void givenAStatusOf(int status) {
        message = new PoolStatusChangedMessage("resilient_pool-0", status);
    }

    private void givenMessageIsReceived() {
        current = PoolStatusForResilience.getStatusFor(message);
    }

    private void givenPoolModeOf(int mode) {
        message.setPoolMode(new PoolV2Mode(mode));
        message.getPoolMode().setReplicationEnabled(!replicationDisabled);
    }

    private void givenPoolStatus(PoolStatusForResilience status) {
        current = status;
        whenTransitionIsComputed();
    }

    private void givenPoolStatusDownMessageWithMode(int mode) {
        givenAStatusOf(PoolStatusChangedMessage.DOWN);
        givenPoolModeOf(mode);
        givenMessageIsReceived();
        whenTransitionIsComputed();
    }

    private void givenPoolStatusRestartMessageWithMode(int mode) {
        givenAStatusOf(PoolStatusChangedMessage.RESTART);
        givenPoolModeOf(mode);
        givenMessageIsReceived();
        whenTransitionIsComputed();
    }

    private void givenPoolStatusUpMessageWithMode(int mode) {
        givenAStatusOf(PoolStatusChangedMessage.UP);
        givenPoolModeOf(mode);
        givenMessageIsReceived();
        whenTransitionIsComputed();
    }

    private void givenReplicationIsDisabled() {
        replicationDisabled = true;
    }

    private void whenTransitionIsComputed() {
        if (last == null) {
            next = current;
        } else {
            next = last.getNext(current);
        }

        last = current;
        current = next;
    }
}
