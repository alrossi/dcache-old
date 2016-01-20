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

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;

import diskCacheV111.util.CacheException;
import org.dcache.resilience.TestBase;
import org.dcache.resilience.TestSynchronousExecutor;
import org.dcache.vehicles.resilience.AddPoolToPoolGroupMessage;
import diskCacheV111.vehicles.AddStorageUnitToGroupsMessage;
import diskCacheV111.vehicles.AddStorageUnitsToGroupMessage;
import org.dcache.vehicles.resilience.ModifyStorageUnitMessage;
import diskCacheV111.vehicles.ReloadPsuMessage;
import org.dcache.vehicles.resilience.RemovePoolFromPoolGroupMessage;
import org.dcache.vehicles.resilience.RemovePoolGroupMessage;
import org.dcache.vehicles.resilience.RemovePoolMessage;
import org.dcache.vehicles.resilience.RemoveStorageUnitFromPoolGroupsMessage;
import org.dcache.vehicles.resilience.RemoveStorageUnitMessage;
import diskCacheV111.vehicles.RemoveStorageUnitsFromGroupMessage;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Created by arossi on 9/21/15.
 */
public final class PoolSelectionUnitDecoratorTest extends TestBase {

    Serializable message;

    @Before
    public void setUp() throws CacheException {
        setUpBase();
        setShortExecutionMode(TestSynchronousExecutor.Mode.RUN);
        createCellStubs();
        setPnfsMessageProcessor(
                        (m) -> PoolSelectionUnitDecoratorTest.this.message = m);
        setPnfsNotifyProcessor(
                        (m) -> PoolSelectionUnitDecoratorTest.this.message = m);
        createPsuDecorator();
        wirePsuDecorator();
        initializePsuDecorator();
        message = null;
    }

    @Test
    public void shouldNotSendAddToUnitGroupMessageWhenAddUnitAddedIsNotStorage() {
        decorator.createUnit("131.225.80.252", true, false, false, false);
        decorator.addToUnitGroup("standard-storage", "131.225.80.252", false);
        assertThatMessageIs(null);
    }

    @Test
    public void shouldNotSendRemoveFromUnitGroupMessageWhenStorageUnitIsNotRemovedFromUnitGroup() {
        decorator.removeFromUnitGroup("world-net", "0.0.0.0/0.0.0.0", true);
        assertThatMessageIs(null);
    }

    @Test
    public void shouldNotSendRemoveUnitMessageWhenUnitRemovedIsNotStorage() {
        decorator.removeUnit("0.0.0.0/0.0.0.0", true);
        assertThatMessageIs(null);
    }

    @Test
    public void shouldSendAddStorageUnitsMessageWhenAddLinkIsCalled() {
        decorator.createLink("new-link",
                             ImmutableList.<String>of("resilient-storage"));
        decorator.addLink("new-link", "resilient-group");
        assertThatMessageIs(AddStorageUnitsToGroupMessage.class);
    }

    @Test
    public void shouldSendAddToPoolGroupMessageWhenAddToPoolGroupIsCalled() {
        decorator.createPool("new-pool", false, false);
        decorator.addToPoolGroup("standard-group", "new-pool");
        assertThatMessageIs(AddPoolToPoolGroupMessage.class);
    }

    @Test
    public void shouldSendAddToUnitGroupMessageWhenNullStorageUnitIsAdded() {
        decorator.createUnit("new-resilient-unit.dcache-devel-test@enstore",
                             false, true, false, false);
        decorator.addToUnitGroup("resilient-storage",
                                 "new-resilient-unit.dcache-devel-test@enstore",
                                 false);
        assertThatMessageIs(AddStorageUnitToGroupsMessage.class);
    }

    @Test
    public void shouldSendAddToUnitGroupMessageWhenResilientStorageUnitIsAdded() {
        decorator.createUnit("new-resilient-unit.dcache-devel-test@enstore",
                             false, true, false, false);
        decorator.setStorageUnit("new-resilient-unit.dcache-devel-test@enstore",
                                 "2", null);
        decorator.addToUnitGroup("resilient-storage",
                                 "new-resilient-unit.dcache-devel-test@enstore",
                                 false);
        assertThatMessageIs(AddStorageUnitToGroupsMessage.class);
    }

    @Test
    public void shouldSendReloadMessageWhenAfterSetupIsCalled() {
        decorator.afterSetup();
        assertThatMessageIs(ReloadPsuMessage.class);
    }

    @Test
    public void shouldSendRemoveFromPoolGroupMessageWhenRemoveFromPoolGroupIsCalled() {
        decorator.removeFromPoolGroup("resilient-group", "resilient_pool-6");
        assertThatMessageIs(RemovePoolFromPoolGroupMessage.class);
    }

    @Test
    public void shouldSendRemoveFromUnitGroupMessageWhenStorageUnitIsRemoved() {
        decorator.removeFromUnitGroup("resilient-storage",
                                      "resilient-2.dcache-devel-test@enstore",
                                      false);
        assertThatMessageIs(RemoveStorageUnitFromPoolGroupsMessage.class);
    }

    @Test
    public void shouldSendRemovePoolGroupMessageWhenRemovePoolGroupIsCalled() {
        decorator.removeFromPoolGroup("standard-group", "standard_pool-0");
        decorator.removeFromPoolGroup("standard-group", "standard_pool-1");
        decorator.removePoolGroup("standard-group");
        assertThatMessageIs(RemovePoolGroupMessage.class);
    }

    @Test
    public void shouldSendRemovePoolMessageWhenRemovePoolIsCalled() {
        decorator.removePool("resilient_pool-4");
        assertThatMessageIs(RemovePoolMessage.class);
    }

    @Test
    public void shouldSendRemoveStorageUnitsMessageWhenRemoveLinkIsCalled() {
        decorator.removeLink("resilient-link");
        assertThatMessageIs(RemoveStorageUnitsFromGroupMessage.class);
    }

    @Test
    public void shouldSendRemoveUnitMessageWhenStorageUnitIsRemoved() {
        decorator.removeUnit("resilient-2.dcache-devel-test@enstore", false);
        assertThatMessageIs(RemoveStorageUnitMessage.class);
    }

    @Test
    public void shouldSendUpdateMessageWhenSetStorageUnitIsCalled() {
        decorator.setStorageUnit("resilient-2.dcache-devel-test@enstore", null,
                        null);
        assertThatMessageIs(ModifyStorageUnitMessage.class);
    }

    private void assertThatMessageIs(Class clzz) {
        if (clzz == null) {
            assertNull(message);
        } else {
            assertNotNull(message);
            assertTrue(clzz.equals(message.getClass()));
        }
    }
}
