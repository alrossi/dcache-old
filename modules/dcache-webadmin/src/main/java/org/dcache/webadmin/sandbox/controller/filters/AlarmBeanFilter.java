package org.dcache.webadmin.sandbox.controller.filters;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;

import org.dcache.alarms.AlarmPriority;
import org.dcache.alarms.dao.LogEntry;
import org.dcache.webadmin.sandbox.controller.DataBeanFilter;
import org.dcache.webadmin.sandbox.controller.beans.AlarmBeanContainer;

public final class AlarmBeanFilter implements DataBeanFilter<LogEntry> {
    private static final long serialVersionUID = 1L;

    private final AlarmBeanContainer bean;
    private final  Map<String, AlarmPriority> priorityMap;

    public AlarmBeanFilter(AlarmBeanContainer bean,
                           Map<String, AlarmPriority> priorityMap) {
        this.bean = Preconditions.checkNotNull(bean);
        this.priorityMap = priorityMap;
    }

    public boolean shouldRemove(LogEntry data) {
        if (data.isClosed() && !bean.isShowClosed()) {
            return true;
        }

        if (data.isAlarm()) {
            AlarmPriority p = priorityMap.get(data.getType());

            if (p == null) {
                /*
                 * This means the alarm definition used to exist
                 * but has been removed (hence it no longer has
                 * a priority), so we do not consider it an alarm any more.
                 */
                data.setAlarm(false);
                bean.addToUpdated(data);
                return false;
            }

            return p.ordinal() < bean.getPriority().ordinal();
        }

        return false;
    }
}
