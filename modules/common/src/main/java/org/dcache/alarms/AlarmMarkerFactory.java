package org.dcache.alarms;

import com.google.common.base.Preconditions;
import org.slf4j.IMarkerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import java.util.Iterator;

/**
 * @author arossi
 *
 */
public final class AlarmMarkerFactory {

    private static final IMarkerFactory factory
        = MarkerFactory.getIMarkerFactory();

    public static Marker getMarker() {
        return getMarker((Severity)null, null);
    }

    public static Marker getMarker(String severity,
                                   String type,
                                   String ... keywords) {
        if (severity == null) {
            return getMarker((Severity)null, type, keywords);
        }

        return getMarker(Severity.valueOf(severity), type, keywords);
    }

    public static Marker getMarker(Severity severity,
                                   String type,
                                   String ... keywords) {
        if (severity == null) {
            severity = Severity.HIGH;
        }

        if (type == null) {
            type = IAlarms.ALARM_MARKER_TYPE_GENERIC;
        }

        Marker alarmMarker = factory.getMarker(IAlarms.ALARM_MARKER);

        Marker severityMarker = factory.getMarker(IAlarms.ALARM_MARKER_SEVERITY);
        Marker alarmSeverity = factory.getDetachedMarker(severity.toString());
        severityMarker.add(alarmSeverity);
        alarmMarker.add(severityMarker);

        Marker typeMarker = factory.getMarker(IAlarms.ALARM_MARKER_TYPE);
        Marker alarmType = factory.getDetachedMarker(type);
        typeMarker.add(alarmType);
        alarmMarker.add(typeMarker);

        if (keywords != null) {
            Marker keyMarker = factory.getMarker(IAlarms.ALARM_MARKER_KEY);
            for (String keyword: keywords) {
                Marker alarmKey = factory.getDetachedMarker(keyword);
                keyMarker.add(alarmKey);
            }
            alarmMarker.add(keyMarker);
        }

        return alarmMarker;
    }

    public static Marker getSubmarker(Marker marker, String name) {
        Preconditions.checkNotNull(marker);
        Preconditions.checkNotNull(name);
        for (Iterator<Marker> m = marker.iterator(); m.hasNext();) {
            Marker next = m.next();
            if (name.equals(next.getName())) {
                return next;
            }
        }
        return null;
    }
}
