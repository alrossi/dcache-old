package org.dcache.testdata;

import org.datanucleus.api.jdo.JDOPersistenceManagerFactory;

import javax.jdo.PersistenceManager;
import javax.jdo.Transaction;

import java.io.IOException;
import java.util.Properties;

public abstract class Store {
    protected final String url;

    protected JDOPersistenceManagerFactory pmf;

    protected Store(String url) throws IOException {
        this.url = url;
        initPersistenceManager();
    }

    public <T> void put(T entry) {
        PersistenceManager insertManager = pmf.getPersistenceManager();
        Transaction tx = insertManager.currentTransaction();
        try {
            tx.begin();
            insertManager.makePersistent(entry);
            tx.commit();
        } finally {
            try {
                if (tx != null && tx.isActive()) {
                    tx.rollback();
                }
            } finally {
                insertManager.close();
            }
        }
    }

    protected void initPersistenceManager() throws IOException {
        Properties properties = new Properties();
        properties.setProperty("datanucleus.ConnectionURL", url);
        setOptionalProperties(properties);
        pmf = new JDOPersistenceManagerFactory(properties);
    }

    protected abstract void setOptionalProperties(Properties properties);
}
