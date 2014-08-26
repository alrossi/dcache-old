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
package org.dcache.alarms.shell;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Marker;

import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.dcache.alarms.AlarmMarkerFactory;
import org.dcache.alarms.PredefinedAlarm;
import org.dcache.util.Args;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Wraps the parsing and management of shell command-line arguments for sending
 * an alarm.  For package use only.
 *
 * @author arossi
 */
final class AlarmArguments {
    private static final String DEFAULT_SOURCE_PATH = "/NA/command-line";
    private static final String DEFAULT_SOURCE = "src://" + SendAlarm.LOCAL_HOST
                    + DEFAULT_SOURCE_PATH;
    private static final String DEFAULT_PORT = "60001";
    private static final String TYPE = "t";
    private static final String SOURCE = "s";
    private static final String DESTINATION = "d";

    static final Map<String, String> HELP_MESSAGES
        = ImmutableMap.of
            (TYPE,        "-t=<type>        (optional): predefined alarm subtype tag",
             SOURCE,      "-s=<source>      (optional): source info uri"
                          + " (i.e., \"src://[host]/[domain]/[service]\")",
             DESTINATION, "-d=<destination> (required): logging server uri"
                          + " (i.e., \"dst://[host]:[port]\"; port may be blank)");

    final Marker marker;
    final String sourceHost;
    final String sourceService;
    final String sourceDomain;
    final String destinationHost;
    final String destinationPort;
    final String message;

    AlarmArguments(Args parsed) throws Exception {
        List<String> arguments = parsed.getArguments();
        checkArgument(arguments.size() > 0,
                        "please provide a non-zero-length alarm message"
                        + "; -h[elp] for options");
        Iterator<String> it = arguments.iterator();
        StringBuilder msg = new StringBuilder(it.next());
        while (it.hasNext()) {
           msg.append(" ").append(it.next());
        }
        message = msg.toString();

        String arg = parsed.getOption(DESTINATION);
        checkNotNull(arg, "please provide a uri:"
                        + SendAlarm.LBRK + SendAlarm.INDENT + HELP_MESSAGES.get(DESTINATION)
                        + "; -h[elp] for options");

        URI uri = new URI(arg);
        destinationHost = uri.getHost();
        checkNotNull(destinationHost,
                        "please provide a host in the uri:"
                        + SendAlarm.LBRK + SendAlarm.INDENT + HELP_MESSAGES.get(DESTINATION)
                        + "; -h[elp] for other options");

        arg = String.valueOf(uri.getPort());
        if ("-1".equals(arg)) {
            destinationPort = DEFAULT_PORT;
        } else {
            destinationPort = arg;
        }

        arg = Strings.emptyToNull(parsed.getOption(SOURCE));
        if (arg != null) {
            uri = new URI(arg);
        } else {
            uri = new URI(DEFAULT_SOURCE);
        }

        arg = Strings.emptyToNull(uri.getHost());
        if (arg == null) {
            arg = SendAlarm.LOCAL_HOST;
        }

        sourceHost = arg;

        arg = Strings.emptyToNull(uri.getPath());
        if (arg == null) {
            arg = DEFAULT_SOURCE_PATH;
        }

        String[] parts = arg.substring(1).split("[/]");
        sourceDomain = parts[0];
        if (parts.length > 1) {
            sourceService = parts[1];
        } else {
            sourceService = sourceDomain;
        }

        PredefinedAlarm type = null;
        arg = Strings.emptyToNull(parsed.getOption(TYPE));
        if (arg != null) {
            try {
                type = PredefinedAlarm.valueOf(arg.toUpperCase());
            } catch (IllegalArgumentException noSuchType) {
                // just allow the null type to stand
            }
        }

        marker = AlarmMarkerFactory.getMarker(type);
    }
}