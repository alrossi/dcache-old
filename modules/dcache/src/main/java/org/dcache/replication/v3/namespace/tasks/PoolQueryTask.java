package org.dcache.replication.v3.namespace.tasks;

/**
 * @author arossi
 *
 */
public class PoolQueryTask {

    /*
     * 1. Get all locations
     * 2. eliminate the pool if DOWN.
     * 3. eliminate locations not in group
     * 4. eliminate locations not active.
     * 4. we can assume the rest are sticky if we force the sticky bit on resilient pools
     *    and remove using rep rm -force.
     * Be sure to set CDC sessionId on this.
     *
     */
}
