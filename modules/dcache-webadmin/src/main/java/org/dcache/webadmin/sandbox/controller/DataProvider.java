package org.dcache.webadmin.sandbox.controller;

import org.apache.wicket.extensions.markup.html.repeater.util.SortableDataProvider;
import org.apache.wicket.model.IModel;
import org.apache.wicket.model.Model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public abstract class DataProvider<D extends Serializable,      // data object
                                   S extends Serializable,      // sort type
                                   B extends DataBeanContainer> // bean container type
        extends SortableDataProvider<D,S> {

    private static final long serialVersionUID = 1L;

    private final List<DataBeanFilter<D>> filterChain = new ArrayList<>();

    protected B containerBean;

    public B getDataContainerBean() {
        return containerBean;
    }

    @Override
    public Iterator<? extends D> iterator(long first, long count) {
        List<D> data = getFilteredList();
        Collections.sort(data, getComparator());
        return data.subList((int)first,
                            (int)Math.min(first + count, data.size())).iterator();
    }

    @Override
    public IModel<D> model(D object) {
        return Model.of(object);
    }

    @Override
    public long size() {
        return getFilteredList().size();
    }

    /**
     * Needs to be overridden by each descendant class which provides any
     * special filters by adding those filters to the list and returning the
     * list, always calling <code>chain=super.buildFilterChain(chain)</code>
     * first.
     */
    protected List<DataBeanFilter<D>> buildFilterChain(List<DataBeanFilter<D>> chain) {
        return chain;
    }

    protected abstract Comparator<D> getComparator();

    private List<D> getFilteredList() {
        filterChain.clear();
        containerBean.filterData(buildFilterChain(filterChain));
        return containerBean.getData();
    }
}
