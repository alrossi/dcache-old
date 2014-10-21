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
package org.dcache.replication.data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.EnumSet;
import java.util.Set;

import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionLink;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPoolGroup;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionUnit;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionUnitGroup;
import diskCacheV111.poolManager.StorageUnit;
import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.StorageInfo;

import dmg.cells.nucleus.CellEndpoint;

import org.dcache.acl.enums.AccessMask;
import org.dcache.cells.CellStub;
import org.dcache.namespace.FileAttribute;
import org.dcache.replication.api.ReplicationQueryUtilities;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.PnfsGetFileAttributes;

/**
 * Expresses the minimum, maximum and same host requirements for either
 * a single pnfsid on a given pool, or for a pool group as a whole.
 *
 * @author arossi
 */
public final class ReplicationConstraints {
    private static final Logger LOGGER
        = LoggerFactory.getLogger(ReplicationConstraints.class);

    private final ReplicationQueryUtilities utils;

    private int minimum = 1;
    private int maximum = 1;
    private boolean sameHostOK = false;

    ReplicationConstraints(ReplicationQueryUtilities utils) {
        this.utils = utils;
    }

    public void copy(ReplicationConstraints constraints) {
        minimum = constraints.minimum;
        maximum = constraints.maximum;
        sameHostOK = constraints.sameHostOK;
    }

    public int getMinimum() {
        return minimum;
    }

    public int getMaximum() {
        return maximum;
    }

    public boolean isSameHostOK() {
        return sameHostOK;
    }

    /**
     * Enforces a simple override of the pool group constraints by those on the
     * file's storage group, if any are expressed.
     *
     * @return whether this file should be replicated.
     */
    public void verifyConstraintsForFile(PnfsIdMetadata opData,
                                         CellStub pnfsManager,
                                         CellEndpoint endpoint,
                                         PoolSelectionUnit psu)
                    throws CacheException, InterruptedException {
        PnfsId pnfsId = opData.pnfsId;
        SelectionPoolGroup poolGroup = opData.poolGroupData.poolGroup;
        PnfsGetFileAttributes attributes = getAttributes(pnfsId, pnfsManager);

        if (!isOnlineAccessLatency(attributes)) {
            return;
        }

        /*
         * do not handle temporary copies
         */
//        if ( !utils.isSystemSticky(pnfsId.toString(), opData.poolName, endpoint)) {
//            return;
//        }

        maximum = poolGroup.getMaxReplicas();
        minimum = poolGroup.getMinReplicas();
        sameHostOK = poolGroup.areSameHostReplicasEnabled();

        StorageInfo storageInfo = attributes.getFileAttributes()
                                            .getStorageInfo();
        String unitKey = storageInfo.getStorageClass();
        String hsm = storageInfo.getHsm();
        if (hsm != null) {
            unitKey += ("@" + hsm);
        }
        StorageUnit sunit = psu.getStorageUnit(unitKey);

        if (sunit != null) {
            Integer smax = sunit.getMaxReplicas();
            Integer smin = sunit.getMinReplicas();

            /*
             * simple override
             */
            if (smax != null) { // both should be valid
                maximum = smax;
                minimum = smin;
            }

            /*
             * simple override
             */
            Boolean sEnabled = sunit.areSameHostReplicasEnabled();
            if (sEnabled != null) {
                sameHostOK = sEnabled;
            }
        }
    }

    /**
     * Attempts to determine the tightest constraints for the pool group by
     * examining all the storage units which may be associated with it. This
     * would mean we look for the greatest lower bound for minimum, and the
     * least upper bound for maximum. Files will subsequently be examined to see
     * if they actually should be replicated or reduced, but this guarantees
     * there will be no false negatives (just false positives).<br>
     * <br>
     * Note that <code>sameHostOK</code> is not relevant to this
     * computation, so this method ignores it.
     */
    public void verifyConstraintsForPoolGroup(SelectionPoolGroup poolGroup,
                                              PoolSelectionUnit psu) {
        maximum = poolGroup.getMaxReplicas();
        minimum = poolGroup.getMinReplicas();

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
                        Integer smax = sunit.getMaxReplicas();
                        Integer smin = sunit.getMinReplicas();
                        if (smax != null) { // both should be valid
                            maximum = Math.min(maximum, smax);
                            minimum = Math.max(minimum, smin);
                        }
                    }
                }
            }
        }
    }

    public String toString() {
        return "{min=" + minimum +
               ", max=" + maximum +
               ", sameHostEnabled=" + sameHostOK + "}";
    }

    private PnfsGetFileAttributes getAttributes(PnfsId pnfsId,
                                                CellStub pnfsManagerStub)
                    throws CacheException, InterruptedException {
        Set<FileAttribute> requestedAttributes
            = EnumSet.of(FileAttribute.ACCESS_LATENCY,
                         FileAttribute.STORAGEINFO);
        Set<AccessMask> accessMask = EnumSet.of(AccessMask.READ_DATA);
        PnfsGetFileAttributes msg
            = new PnfsGetFileAttributes(pnfsId, requestedAttributes);
        msg.setAccessMask(accessMask);

        return pnfsManagerStub.sendAndWait(msg);
    }

    private boolean isOnlineAccessLatency(PnfsGetFileAttributes attributes) {
        FileAttributes fattributes = attributes.getFileAttributes();
        AccessLatency accessLatency = fattributes.getAccessLatency();
        LOGGER.debug("verifyConstraintsForFile, access latency {}", accessLatency);
        return AccessLatency.ONLINE.equals(accessLatency);
    }
}
