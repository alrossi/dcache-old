package org.dcache.webadmin.sandbox.model;

import java.io.Serializable;
import java.util.Collection;

import org.dcache.webadmin.sandbox.model.exceptions.DataAccessException;

public interface DataAccessor<D extends Serializable,    // data object
                              Q extends Serializable> {  // query filter
    /**
     * Main interface method which queries the data source for
     * a collection of data objects.
     *
     * @param filter specifying the query.
     * @return collection of data objects matching query.
     */
    Collection<D> get(Q filter) throws DataAccessException;

    /**
     * Indicate whether the data layer is active or reachable.
     */
    boolean isConnected();

    void initialize();

    void shutdown();
}
