package org.dcache.replication.v3;

import java.util.Collection;

/**
 * Generic interface for choosing element from a collection.
 *
 * @author arossi
 */
public interface SelectionStrategy<T> {
    T select(Collection<T> collection);
}
