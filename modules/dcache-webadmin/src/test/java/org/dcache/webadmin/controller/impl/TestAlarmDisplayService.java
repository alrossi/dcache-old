package org.dcache.webadmin.controller.impl;

import java.util.Map;

import org.dcache.alarms.AlarmPriority;
import org.dcache.alarms.dao.LogEntry;
import org.dcache.alarms.file.FileBackedAlarmPriorityMap;
import org.dcache.alarms.jdom.XmlBackedAlarmDefinitionsMap;
import org.dcache.webadmin.controller.util.AlarmTableProvider;
import org.dcache.webadmin.model.dataaccess.DAOFactory;
import org.dcache.webadmin.view.beans.AbstractRegexFilterBean;
import org.dcache.webadmin.view.beans.AlarmQueryBean;

public class TestAlarmDisplayService extends StandardAlarmDisplayService {

    private static final long serialVersionUID = -7563313187663703950L;

    private final FileBackedAlarmPriorityMap pmap;
    private final AlarmQueryBean alarmQueryBean = new AlarmQueryBean();

    public TestAlarmDisplayService(DAOFactory factory) throws Exception {
        super(factory);
        alarmTableProvider = new AlarmTableProvider(this) {
            private static final long serialVersionUID = 3077908716332980559L;

            @Override
            protected AlarmQueryBean getAlarmQueryBean() {
                return alarmQueryBean;
            }

            @Override
            protected AbstractRegexFilterBean<LogEntry> getRegexBean() {
                return alarmQueryBean;
            }
        };

        pmap = new FileBackedAlarmPriorityMap();
        XmlBackedAlarmDefinitionsMap dmap = new XmlBackedAlarmDefinitionsMap();
        pmap.setDefinitions(dmap);
        pmap.setPropertiesPath("dummy.properties");
        pmap.initialize();
    }

    @Override
    public Map<String, AlarmPriority> getPriorityMap() {
        return pmap.getPriorityMap();
    }

    @Override
    public boolean isConnected() {
        return true;
    }
}