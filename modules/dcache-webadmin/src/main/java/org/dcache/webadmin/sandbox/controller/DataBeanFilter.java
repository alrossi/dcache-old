package org.dcache.webadmin.sandbox.controller;

import java.io.Serializable;

public interface DataBeanFilter<D extends Serializable> extends Serializable {
    boolean shouldRemove(D data);
}
