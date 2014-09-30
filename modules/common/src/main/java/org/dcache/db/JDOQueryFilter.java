package org.dcache.db;

import java.io.Serializable;
import java.util.Arrays;

public class JDOQueryFilter implements Serializable {
    private static final long serialVersionUID = 1L;

    public final String filter;
    public final String parameters;
    public final Object[] values;
    public final Integer rangeStart;
    public final Integer rangeEnd;

    public JDOQueryFilter(String filter,
                          String parameters,
                          Object[] values,
                          Integer rangeStart,
                          Integer rangeEnd) {
        this.filter = filter;
        this.parameters = parameters;
        this.values = values;
        if (rangeStart != null) {
            if (rangeEnd == null) {
                rangeEnd = Integer.MAX_VALUE;
            }
        } else if (rangeEnd != null) {
            if (rangeStart == null) {
                rangeStart = 0;
            }
        }
        this.rangeStart = rangeStart;
        this.rangeEnd = rangeEnd;
    }

    public JDOQueryFilter(String filter,
                          String parameters,
                          Object[] values) {
        this(filter, parameters, values, null, null);
    }

    @Override
    public String toString() {
        return filter   + ", "
                        + parameters
                        + (values == null ? null : ", " + Arrays.asList(values))
                        + (rangeStart != null ? "from " + rangeStart : "")
                        + (rangeEnd != null ? "up to " + rangeEnd : "");
    }
}
