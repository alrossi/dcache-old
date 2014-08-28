package org.dcache.replication.api;

import diskCacheV111.vehicles.PnfsAddCacheLocationMessage;
import diskCacheV111.vehicles.PnfsClearCacheLocationMessage;
import diskCacheV111.vehicles.PnfsModifyCacheLocationMessage;

import org.dcache.vehicles.replication.PnfsScannedCacheLocationsMessage;

/**
 * Defines the type of message that was received and/or
 * initiated an operation.
 *
 * @author arossi
 */
public enum PnfsCacheMessageType {
    ADD, CLEAR, SCAN, INDETERMINATE;

    public void increment(ReplicationStatistics statistics) {
        switch(this) {
            case ADD:
                statistics.increment(ReplicationMessageReceiver.MESSAGES,
                                     ReplicationMessageReceiver.ADD_CACHE_LOCATION_MSGS);
                break;
            case CLEAR:
                statistics.increment(ReplicationMessageReceiver.MESSAGES,
                                     ReplicationMessageReceiver.CLEAR_CACHE_LOCATION_MSGS);
                break;
            default:
        }
    }

    public void incrementFailed(ReplicationStatistics statistics) {
        switch(this) {
            case ADD:
                statistics.incrementFailed(ReplicationMessageReceiver.MESSAGES,
                                           ReplicationMessageReceiver.ADD_CACHE_LOCATION_MSGS);
                break;
            case CLEAR:
                statistics.incrementFailed(ReplicationMessageReceiver.MESSAGES,
                                           ReplicationMessageReceiver.CLEAR_CACHE_LOCATION_MSGS);
                break;
            default:
        }
    }

    public static PnfsCacheMessageType getType(PnfsModifyCacheLocationMessage message) {
        if (message instanceof PnfsAddCacheLocationMessage) {
            return ADD;
        } else if (message instanceof PnfsClearCacheLocationMessage) {
            return CLEAR;
        } else if (message instanceof PnfsScannedCacheLocationsMessage) {
            return SCAN;
        } else {
            return INDETERMINATE;
        }
    }
}