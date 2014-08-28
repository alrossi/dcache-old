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
package org.dcache.replication.messages;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;

import org.dcache.cells.CellStub;
import org.dcache.replication.api.ReplicationCopyMessageFactory;

/**
 * This is a provisional implementation which will probably disappear.  It
 * provides the command for issuing migration copy requests through
 * the command-line interface of the migration module, as well as
 * the the parsing of the reply string which is returned.
 *
 * @author arossi
 */
public final class CLIMigrationCopyMessageFactory extends BaseRequestMessageFactory
                                                  implements ReplicationCopyMessageFactory<String> {
    private static final String MIGRATION_CP = "migration copy";
    private static final Pattern ID_PATTERN
        = Pattern.compile("\\[([\\d]+)\\](.+)");

    @Override
    public String extractRequestIdFromReply(String message) {
        Matcher m = ID_PATTERN.matcher(message);
        if (!m.find()) {
            throw new IllegalStateException("Bad return message: no job id: "
                            + message);
        }
        return m.group(1);
    }

    @Override
    public String createReplicateRequestMessage(PnfsId pnfsId,
                                                String poolGroup,
                                                int numCopies,
                                                boolean sameHostOK) {
//        String exclude = sameHostOK ? "" :
//            " -exclude-when=source.pool=target.pool";
        return new StringBuilder(MIGRATION_CP)
                                 .append(" -pnfsid=").append(pnfsId.toString())
//                                 .append(" -copies=").append(numCopies)
                                 .append(" -target=pgroup")
                                 .append(" -tmode=cached+system")
                                 .append(" -eager=true")
//                                 .append(exclude)
                                 .append(" ")
                                 .append(poolGroup)
                                 .toString();
    }

    @Override
    public Class<String> getMessageClass() {
        return String.class;
    }

    @Override
    public CellStub getTargetStub(String pool) {
        CellStub stub = new CellStub();
        stub.setDestination(pool);
        stub.setTimeout(timeout);
        stub.setTimeoutUnit(timeoutUnit);
        return stub;
    }

    @Override
    public void handleException(Exception exception, Object ... args){
        if (exception instanceof CacheException) {
            LOGGER.warn("Copy request {} failed.",
                         args[0],
                         exception.getMessage());
        } else if (exception instanceof InterruptedException) {
            LOGGER.warn("Copy request {} timed out.",
                         args[0],
                         exception.getMessage());
        } else {
            throw new RuntimeException("Unexpected exception for copy request "
                                        + args[0],
                                        exception);
        }
    }
}
