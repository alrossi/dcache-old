package org.dcache.services.transfermanager.data;

import diskCacheV111.services.TransferManagerHandler;

/**
 * @author arossi
 *
 */
public class TransferManagerHandlerBackup {
    protected long creationTime;
    protected Long credentialId;
    protected long id;
    protected long lifeTime;
    protected Integer moverId;
    protected String pnfsIdString;
    protected String pnfsPath;
    protected String pool;
    protected String remoteUrl;
    protected int state;

    boolean created;
    boolean store;

    transient boolean locked;

    protected TransferManagerHandlerBackup() {

    }

    public TransferManagerHandlerBackup(TransferManagerHandler handler) {
        creationTime = handler.getCreationTime();
        lifeTime     = handler.getLifeTime();
        id           = handler.getId();
        pnfsPath     = handler.getPnfsPath();
        pnfsIdString = handler.getPnfsIdString();
        pool         = handler.getPool();
        store        = handler.getStore();
        created      = handler.getCreated();
        locked       = handler.getLocked();
        remoteUrl    = handler.getRemoteUrl();
        moverId      = handler.getMoverId();
        state        = handler.getState();
        credentialId = handler.getCredentialId();
    }
}
