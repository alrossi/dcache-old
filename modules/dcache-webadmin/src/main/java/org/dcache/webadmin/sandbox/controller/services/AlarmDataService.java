package org.dcache.webadmin.sandbox.controller.services;

import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;

import diskCacheV111.util.CacheException;

import org.dcache.alarms.dao.AlarmQueryUtils;
import org.dcache.alarms.dao.LogEntry;
import org.dcache.cells.CellStub;
import org.dcache.db.JDOQueryFilter;
import org.dcache.util.CacheExceptionFactory;
import org.dcache.vehicles.alarms.AlarmPriorityMapRequestMessage;
import org.dcache.webadmin.sandbox.controller.DataService;
import org.dcache.webadmin.sandbox.controller.beans.AlarmBeanContainer;
import org.dcache.webadmin.sandbox.controller.providers.AlarmTableProvider;
import org.dcache.webadmin.sandbox.model.accessors.DataNucleusAlarmsAccessor;
import org.dcache.webadmin.sandbox.model.exceptions.DataAccessException;

public final class AlarmDataService extends DataService<LogEntry,
                                                        JDOQueryFilter,
                                                        AlarmTableProvider,
                                                        DataNucleusAlarmsAccessor> {
    private CellStub alarmService;

    @Override
    public JDOQueryFilter getFilter(AlarmTableProvider provider) {
        AlarmBeanContainer bean = provider.getDataContainerBean();
        return  AlarmQueryUtils.getFilter(bean.getAfter(),
                                          bean.getBefore(),
                                          bean.getType(),
                                          bean.isAlarm(),
                                          bean.getFrom(),
                                          bean.getTo());
    }

    public void setAlarmService(CellStub alarmService) {
        this.alarmService = alarmService;
    }

    public void shutdown() {
        accessor.shutdown();
    }

    @Override
    protected void afterAccess(AlarmTableProvider provider)
                    throws DataAccessException {
        AlarmPriorityMapRequestMessage request
            = new AlarmPriorityMapRequestMessage();
        try {
            request = alarmService.sendAndWait(request);
            int code = request.getReturnCode();
            if (code != 0) {
                throw CacheExceptionFactory.exceptionOf(code,
                                String.valueOf(request.getErrorObject()));
            }
            provider.setMap(request.getMap());
        } catch (CacheException | InterruptedException t) {
            LoggerFactory.getLogger(this.getClass()).error(
                            "Could not get alarm priority map: {}",
                            t.getMessage());
            provider.setMap(Collections.EMPTY_MAP);
        }

        super.afterAccess(provider);
    }

    @Override
    protected void beforeAccess(AlarmTableProvider provider)
                    throws DataAccessException {
        super.beforeAccess(provider);
        AlarmBeanContainer bean = provider.getDataContainerBean();
        Collection<LogEntry> entries = bean.getToUpdate();
        if (!entries.isEmpty()) {
            accessor.update(entries);
            entries.clear();
        }
        entries = bean.getToDelete();
        if (!entries.isEmpty()) {
            accessor.delete(entries);
            entries.clear();
        }
    }
}
