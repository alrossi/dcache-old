package org.dcache.webadmin.sandbox.model.accessors;

import java.io.Serializable;
import java.util.Collection;

import org.dcache.webadmin.sandbox.model.DAOAccessor;
import org.dcache.webadmin.sandbox.model.exceptions.DataAccessException;

public class NOPAccessor implements DAOAccessor {

    public Collection get(Serializable filter) throws DataAccessException {
        return null;
    }

    public boolean isConnected() {
        return false;
    }

    public void initialize() {
    }

    public void shutdown() {
    }

    public long delete(Collection data) throws DataAccessException {
        return 0;
    }

    public Collection delete(Serializable filter) throws DataAccessException {
        return null;
    }

    public long update(Collection data) throws DataAccessException {
        return 0;
    }
}
