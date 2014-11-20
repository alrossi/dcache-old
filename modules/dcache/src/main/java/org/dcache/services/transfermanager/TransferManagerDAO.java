package org.dcache.services.transfermanager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Transaction;

import org.dcache.services.transfermanager.data.TransferManagerHandler;
import org.dcache.services.transfermanager.data.TransferManagerHandlerBackup;

/**
 * @author arossi
 *
 */
public class TransferManagerDAO {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransferManagerDAO.class);



    /*
     *
     * THIS IS A PROFILE
        public boolean doDbLogging() {
        return false;
        }

        public void setDbLogging(boolean dbLogEnabled) {
        }
     * This goes in Spring private PersistenceManager createPersistenceManager()
     * { // FIXME: Close connection pool and pmf Properties properties = new
     * Properties(); properties.setProperty("datanucleus.PersistenceUnitName",
     * "TransferManager"); HikariConfig config = new HikariConfig();
     * config.setJdbcUrl(_jdbcUrl); config.setUsername(_user);
     * config.setPassword(_pass); JDOPersistenceManagerFactory pmf = new
     * JDOPersistenceManagerFactory( Maps.<String,
     * Object>newHashMap(Maps.fromProperties(properties)));
     * pmf.setConnectionFactory(new AlarmEnabledDataSource(_jdbcUrl,
     * TransferManager.class.getSimpleName(), new HikariDataSource(config)));
     * return pmf.getPersistenceManager(); }
     *
     * private String _jdbcUrl = "jdbc:postgresql://localhost/srmdcache";
     * private String _user = "srmdcache"; private String _pass; private String
     * _pwdFile;
     *
     * AlarmEnabledDatasource check also the close() method
     */

    private PersistenceManagerFactory pmf;

    public void removeHandler(TransferManagerHandler handler) {
        final PersistenceManager persistenceManager = pmf.getPersistenceManager();
        synchronized (persistenceManager) {
            TransferManagerHandlerBackup handlerBackup = new TransferManagerHandlerBackup(
                            handler);
            Transaction tx = persistenceManager.currentTransaction();
            try {
                tx.begin();
                persistenceManager.makePersistent(handler);
                persistenceManager.deletePersistent(handler);
                persistenceManager.makePersistent(handlerBackup);
                tx.commit();
                LOGGER.debug("Handler {} removed from db", handler.getId());
            } catch (Exception e) {
                LOGGER.error("Failed to remove handler {} from database: {}.",
                                handler.getId(), e.toString());
            } finally {
                rollbackIfActive(tx);
            }
        }
    }

    public void addHandler(TransferManagerHandler handler) {
        final PersistenceManager persistenceManager = pmf.getPersistenceManager();
        synchronized (persistenceManager) {
            Transaction tx = persistenceManager.currentTransaction();
            try {
                tx.begin();
                persistenceManager.makePersistent(handler);
                tx.commit();
                LOGGER.debug("Added new handler {} to database.", handler.getId());
            } catch (Exception e) {
                LOGGER.error("Failed to add new handler {} into database: {}.",
                                handler.getId(), e.toString());
            } finally {
                rollbackIfActive(tx);
            }
        }
    }

    public void persist(Object o) {
        final PersistenceManager persistenceManager = pmf.getPersistenceManager();
        synchronized (persistenceManager) {
            Transaction tx = persistenceManager.currentTransaction();
            try {
                tx.begin();
                persistenceManager.makePersistent(o);
                tx.commit();
                LOGGER.debug("[{}]: recording new state of handler.",
                                o);
            } catch (Exception e) {
                LOGGER.error("[{}]: recording new state of handler; {}."
                                + o.toString(), e.toString());
            } finally {
                rollbackIfActive(tx);
            }
        }
    }

    public static void rollbackIfActive(Transaction tx) {
        if (tx != null && tx.isActive()) {
            tx.rollback();
        }
    }
}
