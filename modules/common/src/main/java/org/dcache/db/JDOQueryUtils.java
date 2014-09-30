package org.dcache.db;

import org.slf4j.Logger;

import javax.jdo.FetchPlan;
import javax.jdo.JDOException;
import javax.jdo.PersistenceManager;
import javax.jdo.Query;
import javax.jdo.Transaction;

import java.util.Collection;
import java.util.List;

public class JDOQueryUtils {
    private static final int MAXIMUM_QUERY_RESULTS = 10000;

    public static long delete(PersistenceManager pm,
                              JDOQueryFilter filter,
                              Class daoClass) {
        Query query = createQuery(pm, filter, daoClass);
        return filter.values == null ? query.deletePersistentAll()
                        : query.deletePersistentAll(filter.values);
    }

    public static <D> Collection<D> execute(PersistenceManager pm,
                                            JDOQueryFilter filter,
                                            Class daoClass) {
        Query query = createQuery(pm, filter, daoClass);

        /*
         * 2013/12/11 -- added a range limit guard. This can be hard-coded as
         * effectively the capacity to hold more than 10000 entries in memory
         * should not be required. One can always adjust the numbers or refine
         * the query.
         */
        Integer from = filter.rangeStart == null ? 0 : filter.rangeStart;
        int limit = from + MAXIMUM_QUERY_RESULTS;
        Integer to = filter.rangeEnd == null ? limit
                                             : Math.min(filter.rangeEnd, limit);
        query.setRange(from, to);

        /*
         * evidently required by DataNucleus 3.1.3+ to get most recent updates
         * from other JVMs
         */
        query.setIgnoreCache(true);
        return (Collection<D>) (filter.values == null ? query.execute()
                                : query.executeWithArray(filter.values));
    }

    public static void logJDOException(String action,
                                       JDOQueryFilter filter,
                                       Logger logger,
                                       JDOException e) {
        /*
         * JDOException extends RuntimeException, but we treat it as
         * a checked exception here; the SQL error should neither
         * be dealt with by the client nor be propagated up in this case
         */
        if (filter == null) {
            logger.error("alarm data, failed to {}: {}", action, e.getMessage());
        } else {
            logger.error("alarm data, failed to {}, {}: {}", action, filter, e.getMessage());
        }

        logger.debug("{}", action, e);
    }

    public static void rollbackIfActive(Transaction tx) {
        if (tx.isActive()) {
            tx.rollback();
        }
    }

    public static Object[] emptyListToNull(List<Object> values) {
        return values.isEmpty() ? null : values.toArray();
    }

    /**
     * Construct an actual JDO query from the filter.
     */
    private static Query createQuery(PersistenceManager pm,
                                     JDOQueryFilter filter,
                                     Class daoClass) {
        Query query = pm.newQuery(daoClass);
        query.setFilter(filter.filter);
        query.declareParameters(filter.parameters);
        query.addExtension("datanucleus.query.resultCacheType",
                           "none");
        query.addExtension("datanucleus.rdbms.query.resultSetType",
                           "scroll-insensitive");
        query.getFetchPlan().setFetchSize(FetchPlan.FETCH_SIZE_OPTIMAL);
        return query;
    }

    private JDOQueryUtils() {
    }
}
