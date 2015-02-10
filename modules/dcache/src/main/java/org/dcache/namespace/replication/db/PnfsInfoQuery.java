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
package org.dcache.namespace.replication.db;

import java.text.ParseException;

import org.dcache.namespace.replication.data.PoolGroupInfo;
import org.dcache.namespace.replication.tasks.PnfsIdProcessor;

/**
 * Encapsulates settings and callback objects for processing
 * a pool update query in order to obtain the set of qualifying
 * pnfsids.
 *
 * Created by arossi on 2/5/15.
 */
public final class PnfsInfoQuery {
    public enum BoundCheck {
        UPPER_MIN, LOWER_MAX, UNDEFINED
    }

    public final PoolGroupInfo poolGroupInfo;
    public final PnfsIdProcessor callback;
    public final String poolName;
    public final boolean excludeThisPool;

    private String sql;

    public PnfsInfoQuery(String poolName,
                         PoolGroupInfo poolGroupInfo,
                         PnfsIdProcessor callback,
                         boolean excludeThis) {
        this.poolName = poolName;
        this.callback = callback;
        this.poolGroupInfo = poolGroupInfo;
        this.excludeThisPool = excludeThis;
    }

    public void createSql(String filter) throws ParseException {
        if (filter == null) {
            sql = LocalNamespaceAccess.SQL_GET_PNFSIDS_FOR_LOCATION;
        } else {
            sql = String.format(LocalNamespaceAccess.SQL_GET_LOCATION_COUNTS
                            + " HAVING count(*) %s", validateInequality(filter));
        }
    }

    public String getSql() {
        return sql;
    }

    static String validateInequality(String expression) throws ParseException {
        String validate = expression.trim();

        if (validate.length() < 1) {
            throw new ParseException(expression + " not a valid inequality.", 0);
        }

        switch (validate.charAt(0)) {
            case '=':
            case '<':
            case '>':
                validate = validate.substring(1).trim();
                break;
            default:
                throw new ParseException(expression
                                + " not a valid inequality.", 0);
        }

        if (validate.length() < 1) {
            throw new ParseException(expression + " not a valid inequality.", 1);
        }

        if (validate.charAt(0) == '=') {
            validate = validate.substring(1).trim();
        }

        if (validate.length() < 1) {
            throw new ParseException(expression + " not a valid inequality.", 2);
        }

        try {
            Integer.parseInt(validate);
        } catch (NumberFormatException e) {
            throw new ParseException(expression + " is not an integer.", 2);
        }

        return expression;
    }
}
