package org.dcache.webadmin.sandbox.model.accessors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jdo.JDOException;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Transaction;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.dcache.alarms.dao.AlarmQueryUtils;
import org.dcache.alarms.dao.LogEntry;
import org.dcache.db.JDOQueryFilter;
import org.dcache.db.JDOQueryUtils;
import org.dcache.webadmin.sandbox.model.DAOAccessor;

public class DataNucleusAlarmsAccessor
        implements DAOAccessor<LogEntry, JDOQueryFilter> {
    private static final Logger LOGGER
        = LoggerFactory.getLogger(DataNucleusAlarmsAccessor.class);

    private PersistenceManagerFactory pmf;

    public Collection<LogEntry> delete(JDOQueryFilter filter) {
       throw new UnsupportedOperationException("delete " + filter);
    }

    @Override
    public long delete(Collection<LogEntry> selected) {
        if (selected.isEmpty()) {
            return 0;
        }

        PersistenceManager deleteManager = pmf.getPersistenceManager();
        if (deleteManager == null) {
            return 0;
        }

        Transaction tx = deleteManager.currentTransaction();
        JDOQueryFilter filter = AlarmQueryUtils.getIdFilter(selected);

        try {
            tx.begin();
            long removed = JDOQueryUtils.delete(deleteManager,
                                                filter,
                                                LogEntry.class);
            tx.commit();
            LOGGER.debug("successfully deleted {} entries", removed);
            return removed;
        } catch (JDOException t) {
            JDOQueryUtils.logJDOException("delete", filter, LOGGER, t);
            return 0;
        } finally {
            try {
                JDOQueryUtils.rollbackIfActive(tx);
            } finally {
                deleteManager.close();
            }
        }
    }

    public Collection<LogEntry> get(JDOQueryFilter filter) {
        PersistenceManager readManager = pmf.getPersistenceManager();
        if (readManager == null) {
            return Collections.emptyList();
        }

        Transaction tx = readManager.currentTransaction();

        try {
            tx.begin();
            Collection<LogEntry> result = JDOQueryUtils.execute(readManager,
                                                                filter,
                                                                LogEntry.class);
            Collection<LogEntry> detached = readManager.detachCopyAll(result);
            LOGGER.debug("got detatched collection {}", detached);
            tx.commit();
            LOGGER.debug("successfully executed get for filter {}", filter);
            return detached;
        } catch (JDOException t) {
            JDOQueryUtils.logJDOException("get", filter, LOGGER, t);
            return Collections.emptyList();
        } finally {
            try {
                JDOQueryUtils.rollbackIfActive(tx);
            } finally {
                readManager.close();
            }
        }
    }

    public boolean isConnected() {
        return pmf != null && pmf.getPersistenceManager() != null;
    }


    public void setPersistenceManagerFactory(PersistenceManagerFactory pmf) {
        this.pmf = pmf;
    }

    @Override
    public long update(Collection<LogEntry> selected) {
        if (selected.isEmpty()) {
            return 0;
        }

        PersistenceManager updateManager = pmf.getPersistenceManager();
        if (updateManager == null) {
            return 0;
        }

        Transaction tx = updateManager.currentTransaction();
        JDOQueryFilter filter = AlarmQueryUtils.getIdFilter(selected);

        try {
            tx.begin();
            Collection<LogEntry> result = JDOQueryUtils.execute(updateManager,
                                                                filter,
                                                                LogEntry.class);
            LOGGER.debug("got matching entries {}", result);
            long updated = result.size();

            Map<String, LogEntry> map = new HashMap<>();
            for (LogEntry e : selected) {
                map.put(e.getKey(), e);
            }

            for (LogEntry e : result) {
                e.update(map.get(e.getKey()));
            }

            /*
             * result is not detached so it will be updated on commit
             */
            tx.commit();
            LOGGER.debug("successfully updated {} entries", updated);

            return updated;
        } catch (JDOException t) {
            JDOQueryUtils.logJDOException("update", filter, LOGGER, t);
            return 0;
        } finally {
            try {
                JDOQueryUtils.rollbackIfActive(tx);
            } finally {
                updateManager.close();
            }
        }
    }

    public void initialize() {
    }

    public void shutdown() {
    }
}
