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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import diskCacheV111.poolManager.CostModule;
import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionLink;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPoolGroup;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionUnit;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionUnitGroup;
import diskCacheV111.poolManager.StorageUnit;

/**
 * <p>Utilities for finding storage unit info relevant to resilient pool group
 *    operations.</p>
 *
 * Created by arossi on 2/25/15.
 */
public final class StorageUnitInfoExtractor {
    /**
     * @return all the resilient pool groups to which this storage unit is linked.
     */
    public static Collection<String> getResilientGroupsFor(String unitName,
                                                           PoolSelectionUnit psu) {
        return psu.getPoolGroups().values().stream()
          .filter((g) -> g.isResilient()
                          && hasStorageUnit(g.getName(), unitName, psu))
          .map((g) -> g.getName())
          .collect(Collectors.toList());
    }

    /**
     * @return all the storage units linked to this pool group.
     */
    public static Collection<StorageUnit> getStorageUnitsInGroup(String name,
                                                                 PoolSelectionUnit psu) {
        Collection<StorageUnit> units = new ArrayList<>();
        psu.getLinksPointingToPoolGroup(name).stream().forEach((link) ->
            link.getUnitGroupsTargetedBy().stream().forEach((ugroup) ->
                ugroup.getMemeberUnits().stream().forEach((unit) -> {
                    if (unit instanceof StorageUnit) {
                        units.add((StorageUnit) unit);
                    }
                })));  // LISP strikes back!
        return units;
    }

    /**
     * @return true if the pool group has at least one storage group
     * associated with it via a link, and the storage group overrides the
     * pool group resilience settings for required number of copies
     * (via a non-<code>null</code> value).
     */
    public static boolean hasResilientStorageUnit(String poolGroup,
                                                  PoolSelectionUnit psu) {
        Collection<SelectionLink> links = psu.getLinksPointingToPoolGroup(
                        poolGroup);
        for (SelectionLink link : links) {
            Collection<SelectionUnitGroup> unitGroups = link.getUnitGroupsTargetedBy();
            for (SelectionUnitGroup ugroup : unitGroups) {
                Collection<SelectionUnit> units = ugroup.getMemeberUnits();
                for (SelectionUnit unit : units) {
                    if (unit instanceof StorageUnit) {
                        StorageUnit sunit = (StorageUnit)unit;
                        if (sunit.getRequiredCopies() > 1) {
                            return true;
                        }
                    }
                }
            }
        }

        return false;
    }

    /**
     * Checks to make sure that a resilient storage unit is not associated
     * with non-resilient pool groups.
     */
    public static void validate(StorageUnit unit, PoolSelectionUnit psu) {
        validate(unit, getNonResilientGroups(psu), psu);
    }

    /**
     * Checks to make sure that resilient storage units are not associated
     * with non-resilient pool groups.
     */
    public static void validateAllStorageUnits(PoolSelectionUnit psu) {
        Set<SelectionPoolGroup> nonresilient = getNonResilientGroups(psu);

        psu.getUnitGroups().values()
                           .stream()
                           .forEach((ug) -> ug.getMemeberUnits().stream()
                                           .filter((u) -> u instanceof StorageUnit)
                                           .map((u) -> (StorageUnit) u)
                                           .forEach((u) -> validate(u,
                                                                    nonresilient,
                                                                    psu)));
    }

    /**
     * Checks to make sure that resilient storage unit constraints can
     * be met by all resilient groups to which it is linked.
     */
    public static void verifyCanBeSatisfied(StorageUnit unit,
                                            PoolSelectionUnit psu,
                                            CostModule module) {
        getResilientGroupsFor(unit.getName(), psu).stream()
                            .forEach((group) -> {
                                short storageRequired = unit.getRequiredCopies();
                                CostModuleLocationExtractor extractor
                                                = new CostModuleLocationExtractor(
                                                unit.getOnlyOneCopyPer(),
                                                module);
                                verify(group, extractor, storageRequired, psu);
                            });
    }

    private static Set<SelectionPoolGroup> getNonResilientGroups(PoolSelectionUnit psu) {
        return psu.getPoolGroups().values().stream()
                        .filter((g) -> !g.isResilient())
                        .collect(Collectors.toSet());
    }

    private static boolean hasStorageUnit(String poolGroup,
                                          String storageUnit,
                                          PoolSelectionUnit psu) {
        Collection<SelectionLink> links = psu.getLinksPointingToPoolGroup(
                        poolGroup);
        for (SelectionLink link : links) {
            Collection<SelectionUnitGroup> unitGroups = link.getUnitGroupsTargetedBy();
            for (SelectionUnitGroup ugroup : unitGroups) {
                Collection<SelectionUnit> units = ugroup.getMemeberUnits();
                for (SelectionUnit unit : units) {
                    if (unit instanceof StorageUnit) {
                        StorageUnit sunit = (StorageUnit)unit;
                        if (sunit.getName().equals(storageUnit)) {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }

    /**
     * <p> We should not allow resilient storage units to be linked to
     * non-resilient pool groups, because the behavior is undefined -- that is,
     * the storage class is attempting to turn the pool group into a resilient
     * one.  This would defeat the use of pool groups to determine which pools
     * to select for replication. </p>
     *
     * <p>Note also that a pool group is defined as resilient or non-resilient
     *    for its lifetime (the attribute cannot be changed).</p>
     */
    private static void validate(StorageUnit unit,
                                 Set<SelectionPoolGroup> nonresilient,
                                 PoolSelectionUnit psu) {
        if (unit.getRequiredCopies() == 1) {
            return;
        }

        List<String> invalid
                        = nonresilient.stream()
                                      .filter((g) -> hasStorageUnit(g.getName(),
                                                      unit.getName(), psu))
                                      .map((g) -> g.getName())
                                      .collect(Collectors.toList());
        if (!invalid.isEmpty()) {
            throw new IllegalStateException(String.format("Resilient storage unit %s "
                            + "is linked to the following non-resilient pool groups: %s; "
                            + "this would potentially cause inconsistent behaviour "
                            + "and is thus prohibited.", unit.getName(), nonresilient));
        }
    }

    /**
     * @param poolGroup to which storage unit is linked.
     * @param extractor configured for the specific tag constraints.
     * @param required  specific to this storage unit.
     * @throws IllegalStateException upon encountering the first set of
     *                               constraints which cannot be met.
     */
    private static void verify(String poolGroup,
                               CostModuleLocationExtractor extractor,
                               int required,
                               PoolSelectionUnit psu) throws IllegalStateException {
        Set<String> members = psu.getPoolsByPoolGroup(poolGroup).stream()
                                 .map((sg) -> sg.getName())
                                 .collect(Collectors.toSet());

        for (int i = 0; i < required; i++) {
            Collection<String> candidates
                            = extractor.getCandidateLocations(members);

            if (candidates.isEmpty()) {
                throw new IllegalStateException(poolGroup);
            }

            String selected = RandomSelectionStrategy.SELECTOR.select(candidates);
            members.remove(selected);
            extractor.addSeenTagsFor(selected);
        }
    }

    private StorageUnitInfoExtractor() {
    }
}
