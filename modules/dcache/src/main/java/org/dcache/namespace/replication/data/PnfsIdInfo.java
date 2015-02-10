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
package org.dcache.namespace.replication.data;

import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

import diskCacheV111.namespace.NameSpaceProvider;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPoolGroup;
import diskCacheV111.poolManager.StorageUnit;
import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import org.dcache.auth.Subjects;
import org.dcache.namespace.FileAttribute;
import org.dcache.vehicles.FileAttributes;

/**
 * Encapsulates all the metadata concerning a given PnfsId/replica
 * needed by replication, along with methods for refreshing
 * it against the namespace provider.
 * <p/>
 * The refresh on locations is provided so that a second check
 * can be made just before selection of a source for copying or
 * for candidate copies for elimination.
 *
 * Created by arossi on 1/29/15.
 */
public final class PnfsIdInfo {
    private static final Set<FileAttribute> REQUIRED_ATTRIBUTES
                    = Collections.unmodifiableSet
                        (EnumSet.of(FileAttribute.ACCESS_LATENCY,
                                    FileAttribute.RETENTION_POLICY,
                                    FileAttribute.STORAGECLASS,
                                    FileAttribute.HSM,
                                    FileAttribute.LOCATIONS));

    public final PnfsId pnfsId;

    private FileAttributes attributes;
    private Collection<String> locations;
    private StorageUnit storageUnit;

    /*
     * Constraints determined by pool group and storage unit.
     */
    private int maximum = 1;
    private int minimum = 1;
    private String onlyOneCopyPer;

    public PnfsIdInfo(PnfsId pnfsId) {
        this.pnfsId = pnfsId;
    }

    public FileAttributes getAttributes() {
        return attributes;
    }

    public void getInfo(StringBuilder builder) {
        builder.append(pnfsId)
               .append("\n\t(sunit ").append(storageUnit)
               .append(")\n\t(min ").append(minimum)
               .append(")\n\t(min ").append(maximum)
               .append(")\n\t(1per ").append(onlyOneCopyPer)
               .append(")\n\t(online )").append(isOnline())
               .append(")\n\t(loc: ").append(locations)
               .append(")");
    }

    public Collection<String> getLocations() {
        return locations;
    }

    public Integer getMaximum() {
        return maximum;
    }

    public Integer getMinimum() {
        return minimum;
    }

    public String getOnlyOneCopyPer() {
        return onlyOneCopyPer;
    }

    public StorageUnit getStorageUnit(PoolGroupInfo poolGroupInfo) {
        if (storageUnit == null) {
            String unitKey = attributes.getStorageClass();
            String hsm = attributes.getHsm();
            if (hsm != null) {
                unitKey += ("@" + hsm);
            }
            storageUnit =  poolGroupInfo.getStorageUnit(unitKey);
        }
        return storageUnit;
    }

    public boolean isOnline() {
        return attributes != null
                        && attributes.getAccessLatency() == AccessLatency.ONLINE;
    }

    public synchronized Collection<String> refreshLocations(NameSpaceProvider provider)
                    throws CacheException {
        locations.clear();
        locations.addAll(provider.getCacheLocation(Subjects.ROOT, pnfsId));
        return locations;
    }

    public synchronized PnfsIdInfo setAttributes(NameSpaceProvider provider)
                    throws CacheException {
        attributes = provider.getFileAttributes(Subjects.ROOT,
                                                pnfsId,
                                                REQUIRED_ATTRIBUTES);
        locations = attributes.getLocations();
        return this;
    }

    /**
     * Determine replication contraints:  minimum/maximum copies required,
     * and whether replicas have special constraints limiting them to
     * one per category (such as host, rack, etc.).
     *
     * @param poolGroupInfo for resilient group having default constraints.
     */
    public PnfsIdInfo setConstraints(PoolGroupInfo poolGroupInfo) {
        SelectionPoolGroup poolGroup = poolGroupInfo.getPoolGroup();
        minimum = poolGroup.getMinReplicas();
        maximum = poolGroup.getMaxReplicas();
        onlyOneCopyPer = poolGroup.getOnlyOneCopyPer();

        StorageUnit sunit = getStorageUnit(poolGroupInfo);

        if (sunit != null) {
            Integer smin = sunit.getMinReplicas();
            Integer smax = sunit.getMaxReplicas();

            if (smin != null) {
                minimum = smin;
            }

            if (smax != null) {
                maximum = smax;
            }

            String oneCopyPer = sunit.getOnlyOneCopyPer();
            if (oneCopyPer != null) {
                onlyOneCopyPer = oneCopyPer;
            }
        }
        return this;
    }

    public String toString() {
        StringBuilder info = new StringBuilder();
        getInfo(info);
        return info.toString();
    }
}
