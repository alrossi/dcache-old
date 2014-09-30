package org.dcache.webadmin.sandbox.controller.services;

import java.io.Serializable;

import org.dcache.webadmin.sandbox.controller.DataProvider;
import org.dcache.webadmin.sandbox.controller.DataService;

public class NOPService extends DataService {
    @Override
    public void initialize() {
    }

    @Override
    public void shutdown() {
    }

    protected Serializable getFilter(DataProvider provider) {
        return null;
    }
}
