package org.dcache.webadmin.sandbox.controller.filters;

import com.google.common.base.Preconditions;

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.dcache.util.IRegexFilterable;
import org.dcache.webadmin.sandbox.controller.DataBeanFilter;
import org.dcache.webadmin.sandbox.controller.beans.RegexFilterableBeanContainer;

public final class RegexBeanFilter<D extends IRegexFilterable>
        implements DataBeanFilter<D> {
    private static final long serialVersionUID = 1L;

    private final RegexFilterableBeanContainer<D> bean;

    public RegexBeanFilter(RegexFilterableBeanContainer<D> bean) {
        this.bean
            = Preconditions.checkNotNull(Preconditions.checkNotNull(bean));
    }

    /**
     * Should never be called with a <code>null</code> expression field on the
     * bean.
     */
    public boolean shouldRemove(D data) {
        if (bean.isRegex()) {
            try {
                Pattern pattern = Pattern.compile(bean.getExpression(),
                                                  bean.getFlags());
                if (!pattern.matcher(data.toFilterableString()).find()) {
                    return true;
                }
            } catch (PatternSyntaxException e) {
                throw new IllegalArgumentException(e.getMessage(), e.getCause());
            }
        } else {
            if (!data.toFilterableString().contains(bean.getExpression())) {
                return true;
            }
        }
        return false;
    }
}
