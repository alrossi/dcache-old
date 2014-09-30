package org.dcache.webadmin.sandbox.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collection;

import org.dcache.util.IRegexFilterable;
import org.dcache.webadmin.sandbox.model.DataAccessor;
import org.dcache.webadmin.sandbox.model.exceptions.DataAccessException;

public abstract class DataService<D extends IRegexFilterable, // data type
                                  Q extends Serializable,     // query type
                                  P extends DataProvider,     // provider type
                                  A extends DataAccessor> {   // accessor type
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    protected A accessor;

    public void initialize() {
        // NOP for base class
    }

    public boolean isConnected() {
        return accessor.isConnected();
    }

    public void refresh(P provider) {
        if (!isConnected()) {
            logger.info("Not connected to service; skipping refresh.");
            return;
        }

        try {
            beforeAccess(provider);
            Collection<D> refreshed = accessor.get(getFilter(provider));
            provider.getDataContainerBean().refreshData(refreshed);
            afterAccess(provider);
        } catch (DataAccessException t) {
            logger.error("Error while refreshing data: page may not "
                            + "show consistently filtered results. "
                            + "Set level to debug to see full stack trace");
            logger.debug("Refresh error", t);
        }
    }

    /**
     * @param accessor to the actual data layer which can be
     *        local storage or data collected via communication
     *        with other cell services.
     */
    public void setAccessor(A accessor) {
        this.accessor = accessor;
    }

    public void shutdown() {
        // NOP for base class
    }
    /**
     * Extra processing which should be done before the call
     * to the data accessor.
     */
    protected void afterAccess(P provider)
                    throws DataAccessException {
        // NOP for base class
    }

    /**
     * Extra processing which should be done after the call
     * to the data accessor.
     */
    protected void beforeAccess(P provider)
                    throws DataAccessException {
        // NOP for base class
    }

    protected abstract Q getFilter(P provider);
}
