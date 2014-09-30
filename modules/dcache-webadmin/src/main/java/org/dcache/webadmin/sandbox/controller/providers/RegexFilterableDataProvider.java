package org.dcache.webadmin.sandbox.controller.providers;

import java.io.Serializable;
import java.util.List;

import org.dcache.util.IRegexFilterable;
import org.dcache.webadmin.sandbox.controller.DataBeanFilter;
import org.dcache.webadmin.sandbox.controller.DataProvider;
import org.dcache.webadmin.sandbox.controller.beans.RegexFilterableBeanContainer;
import org.dcache.webadmin.sandbox.controller.filters.RegexBeanFilter;

public abstract class RegexFilterableDataProvider<D extends IRegexFilterable,
                                                  S extends Serializable,
                                                  B extends RegexFilterableBeanContainer<D>>
        extends DataProvider<D,S,B> {

    private static final long serialVersionUID = 1L;

    protected List<DataBeanFilter<D>> buildFilterChain(List<DataBeanFilter<D>> chain) {
        chain = super.buildFilterChain(chain);
        if (containerBean.getExpression() != null) {
            chain.add(new RegexBeanFilter(containerBean));
        }
        return chain;
    }
}
