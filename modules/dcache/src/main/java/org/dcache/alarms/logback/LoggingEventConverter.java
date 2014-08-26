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
package org.dcache.alarms.logback;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import com.google.common.base.Preconditions;
import org.slf4j.Marker;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.regex.Pattern;

import org.dcache.alarms.AlarmDefinition;
import org.dcache.alarms.AlarmDefinitionsMap;
import org.dcache.alarms.AlarmMarkerFactory;
import org.dcache.alarms.AlarmProperties;
import org.dcache.alarms.dao.LogEntry;

/**
 * This class provides the binding between the logback-specific
 * event structure, the alarm definition and the DAO storage class.
 * It is meant for package-only access.
 *
 * @author arossi
 */
public final class LoggingEventConverter {
    static final LogEntry LOCAL_EVENT = new LogEntry();

    private static String getKeyFromMarker(Marker marker) {
        Marker keyMarker
            = AlarmMarkerFactory.getSubmarker(marker,
                                              AlarmProperties.ALARM_MARKER_KEY);
        if (keyMarker == null) {
            return null;
        }

        StringBuilder builder = new StringBuilder();
        for (Iterator<Marker> it = keyMarker.iterator(); it.hasNext(); ) {
            builder.append(it.next().getName());
        }

        return builder.toString();
    }

    private static String getTypeFromMarker(Marker marker) {
        Marker typeMarker
            = AlarmMarkerFactory.getSubmarker(marker,
                                              AlarmProperties.ALARM_MARKER_TYPE);
        Preconditions.checkNotNull(typeMarker);
        Marker alarmType = (Marker) typeMarker.iterator().next();
        Preconditions.checkNotNull(alarmType);
        return alarmType.getName();
    }

    /*
     * When the alarm is generated at the origin using a marker,
     * its key is determined directly by the value of the key
     * submarker.  This is distinct from the key generation
     * based on attributes and/or regex groups used when the
     * alarm is matched against a definition.
     */
    private static void setTypeAndKeyMetadata(ILoggingEvent event,
                                       AlarmDefinition definition,
                                       LogEntry entry) {
        Level level = event.getLevel();
        String type;
        String key;
        if (definition == null) {
            Marker marker = event.getMarker();
            if (marker != null) {
                type = getTypeFromMarker(marker);
                key = getKeyFromMarker(marker);
                if (key == null) {
                    key = UUID.randomUUID().toString();
                }
                /*
                 * The key is type-specific in this case.
                 */
                key = type + ":" + key;
            } else {
                type = level.toString();
                key = UUID.randomUUID().toString();
            }
        } else {
            type = definition.getType();
            key = definition.createKey(event.getFormattedMessage(),
                                    event.getTimeStamp(),
                                    entry.getHost(),
                                    entry.getDomain(),
                                    entry.getService());
        }

        entry.setType(type);
        entry.setKey(key);
    }

    private AlarmDefinitionsMap definitionsMapping;
    private String serviceName;

    public void setDefinitions(AlarmDefinitionsMap definitionsMapping) {
        this.definitionsMapping = definitionsMapping;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    /**
     * The single entry package method.
     *
     * @param event received by appender
     * @return storage object with all metadata set
     */
    LogEntry createEntryFromEvent(ILoggingEvent event) {
        Map<String, String> mdc = event.getMDCPropertyMap();

        String service = mdc.get(AlarmProperties.SERVICE_TAG);

        if (service == null) {
            service = AlarmProperties.UNKNOWN_SERVICE;
        } else if (service.equals(serviceName)) {
            /*
             * exclude internal events
             */
            return LOCAL_EVENT;
        }

        String host = mdc.get(AlarmProperties.HOST_TAG);
        if (host == null) {
            host = AlarmProperties.UNKNOWN_HOST;
        }

        String domain = mdc.get(AlarmProperties.DOMAIN_TAG);
        if (domain == null) {
            domain = AlarmProperties.UNKNOWN_DOMAIN;
        }

        LogEntry entry = new LogEntry();
        Long timestamp = event.getTimeStamp();
        entry.setFirstArrived(timestamp);
        entry.setLastUpdate(timestamp);
        entry.setInfo(event.getFormattedMessage());
        entry.setHost(host);
        entry.setDomain(domain);
        entry.setService(service);

        AlarmDefinition match = determineIfAlarm(event, entry);

        setTypeAndKeyMetadata(event, match, entry);
        return entry;
    }

    private AlarmDefinition determineIfAlarm(ILoggingEvent event,
                                             LogEntry entry) {
        Marker marker = event.getMarker();
        boolean alarm = marker == null ? false
                        : marker.contains(AlarmProperties.ALARM_MARKER);

        AlarmDefinition match = null;
        if (!alarm) {
            try {
                match = findMatchingDefinition(event);
                alarm = true;
            } catch (NoSuchElementException none) {
            }
        }
        entry.setAlarm(alarm);
        return match;
    }

    private AlarmDefinition findMatchingDefinition(ILoggingEvent event)
                    throws NoSuchElementException {
        Collection<AlarmDefinition> definitions
            = definitionsMapping.getDefinitions();
        for (AlarmDefinition definition : definitions) {
            if (matches(event, definition)) {
                return definition;
            }
        }
        throw new NoSuchElementException("no match for "
                        + event.getFormattedMessage());
    }

    private boolean matches(ILoggingEvent event,
                            AlarmDefinition definition) {
        Pattern regex = definition.getRegexPattern();
        if (regex.matcher(event.getFormattedMessage()).find()) {
            return true;
        }
        Integer depth = definition.getDepth();

        int d = depth == null ? Integer.MAX_VALUE : depth - 1;

        if (definition.isMatchException()) {
            IThrowableProxy p = event.getThrowableProxy();
            while (p != null && d >= 0) {
                if (regex.matcher(p.getMessage()).find()) {
                    return true;
                }
                p = p.getCause();
                d--;
            }
        }
        return false;
    }
}
