package org.dcache.namespace.replication;

import java.util.Collection;

/**
 * Generic interface for choosing element from a collection.
 *
 * @author arossi
 */
public interface CollectionElementSelectionStrategy<T> {
    T select(Collection<T> collection);
}
