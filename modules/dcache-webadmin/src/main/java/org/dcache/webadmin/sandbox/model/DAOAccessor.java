package org.dcache.webadmin.sandbox.model;

import java.io.Serializable;
import java.util.Collection;

import org.dcache.webadmin.sandbox.model.exceptions.DataAccessException;

public interface DAOAccessor<D extends Serializable,
                             F extends Serializable> extends DataAccessor<D, F> {
   long delete(Collection<D> data) throws DataAccessException;

   Collection<D> delete(F filter) throws DataAccessException;

   long update(Collection<D> data) throws DataAccessException;
}
