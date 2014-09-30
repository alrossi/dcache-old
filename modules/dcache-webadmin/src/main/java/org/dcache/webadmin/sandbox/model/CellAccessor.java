package org.dcache.webadmin.sandbox.model;

import java.io.Serializable;

import org.dcache.cells.CellStub;

public interface CellAccessor<D extends Serializable,
                              F extends Serializable> extends DataAccessor<D, F> {
    void setTarget(CellStub stub);
}
