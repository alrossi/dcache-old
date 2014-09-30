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
package org.dcache.webadmin.sandbox.controller.providers;

import org.apache.wicket.extensions.markup.html.repeater.util.SortParam;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.dcache.alarms.AlarmPriority;
import org.dcache.alarms.dao.LogEntry;
import org.dcache.webadmin.sandbox.controller.DataBeanFilter;
import org.dcache.webadmin.sandbox.controller.beans.AlarmBeanContainer;
import org.dcache.webadmin.sandbox.controller.filters.AlarmBeanFilter;

public class AlarmTableProvider
        extends  RegexFilterableDataProvider<LogEntry,
                                             String,
                                             AlarmBeanContainer> {
    private static final long serialVersionUID = 402824287543303781L;

    private Map<String, AlarmPriority> map;

    public void setMap(Map<String, AlarmPriority> map) {
        this.map = map;
    }

    protected List<DataBeanFilter<LogEntry>> buildFilterChain(List<DataBeanFilter<LogEntry>> chain) {
        chain = super.buildFilterChain(chain);
        chain.add(new AlarmBeanFilter(containerBean, map));
        return chain;
    }

    @Override
    protected Comparator<LogEntry> getComparator() {
        return new Comparator<LogEntry>() {

            @Override
            public int compare(LogEntry entry0, LogEntry entry1) {
                SortParam<String> sort = getSort();
                int dir;
                String property;
                if (sort == null) {
                    dir = -1;
                    property = "last";
                } else {
                    dir = sort.isAscending() ? 1 : -1;
                    property = sort.getProperty();
                }

                /*
                 * sort by priority first
                 */
                int priority0 = -1;
                if (entry0.isAlarm()) {
                    AlarmPriority p = map.get(entry0.getType());
                    if (p != null) {
                        priority0 = p.ordinal();
                    }
                }

                int priority1 = -1;
                if (entry1.isAlarm()) {
                    AlarmPriority p = map.get(entry1.getType());
                    if (p != null) {
                        priority1 = p.ordinal();
                    }
                }

                int priority = compare(dir, priority0, priority1);
                if (priority != 0) {
                    return priority;
                }


                switch (property) {
                    case "first":
                        return compare(dir, entry0.getDateOfFirstArrival(),
                                            entry1.getDateOfFirstArrival());
                    case "last":
                        return compare(dir, entry0.getDateOfLastUpdate(),
                                            entry1.getDateOfLastUpdate());
                    case "type":
                        return compare(dir, entry0.getType(), entry1.getType());
                    case "count":
                        return compare(dir, entry0.getReceived(),
                                            entry1.getReceived());
                    case "host":
                        return compare(dir, entry0.getHost(), entry1.getHost());
                    case "domain":
                        return compare(dir, entry0.getDomain(),
                                            entry1.getDomain());
                    case "service":
                        return compare(dir, entry0.getService(),
                                            entry1.getService());
                    default:
                        return 0;
                }
            }

            private <T extends Comparable<T>> int compare(int dir, T a, T b) {
                return (a == null) ? dir : dir * a.compareTo(b);
            }
        };
    }
}
