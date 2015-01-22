package org.dcache.util.replication;

import java.util.concurrent.TimeUnit;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * Created by arossi on 1/22/15.
 */
public abstract class AbstractResilientInfoCache<K,V> {
    protected Cache<K, V> cache;
    protected int expiry = 1;
    protected int size = 1000;
    protected TimeUnit expiryUnit = TimeUnit.MINUTES;

    public void initialize() throws IllegalArgumentException {
        if (expiry < 0) {
            throw new IllegalArgumentException("Cache life must be positive "
                            + "integer; was: " + expiry);
        }

        if (size < 1) {
            throw new IllegalArgumentException("Cache size must be non-zero "
                            + "positive integer; was: " + size);
        }

        cache = CacheBuilder.newBuilder()
                        .expireAfterWrite(expiry, expiryUnit)
                        .maximumSize(size)
                        .softValues()
                        .build();
    }

    public void setExpiry(int expiry) {
        this.expiry = expiry;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public void setExpiryUnit(TimeUnit expiryUnit) {
        this.expiryUnit = expiryUnit;
    }
}
