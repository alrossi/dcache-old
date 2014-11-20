package org.dcache.services.transfermanager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.DoorTransferFinishedMessage;
import diskCacheV111.vehicles.IpProtocolInfo;
import diskCacheV111.vehicles.transferManager.CancelTransferMessage;
import diskCacheV111.vehicles.transferManager.TransferManagerMessage;
import diskCacheV111.vehicles.transferManager.TransferStatusQueryMessage;

import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.CellMessageSender;
import dmg.util.TimebasedCounter;

import org.dcache.cells.CellStub;
import org.dcache.services.transfermanager.data.TransferManagerHandler;
import org.dcache.util.CDCExecutorServiceDecorator;

/**
 * Base class for services that transfer files on behalf of SRM. Used to
 * implement server-side srmCopy.
 */
public abstract class TransferManager implements CellMessageReceiver, CellMessageSender {
    protected static long nextMessageID;
    private static final Logger log = LoggerFactory.getLogger(TransferManager.class);

    public final Set<PnfsId> justRequestedIDs = new HashSet<>();

    private final Map<Long, TransferManagerHandler> activeTransfers = new ConcurrentHashMap<>();
    private final Timer moverTimeoutTimer = new Timer("Mover timeout timer", true);
    private final Map<Long, TimerTask> moverTimeoutTimerTasks = new ConcurrentHashMap<>();
    private final ExecutorService executor = new CDCExecutorServiceDecorator<>(Executors.newCachedThreadPool());


    private TimebasedCounter idGenerator = new TimebasedCounter();

    private CellEndpoint endpoint;
    private CellStub billing;
    private boolean doDatabaseLogging;

    private TransferManagerDAO dao;

    private String ioQueueName; // multi io queue option
    private int maxNumberOfDeleteRetries;
    private int maxTransfers;
    private long moverTimeout;
    // this is the timer which will timeout the
    // transfer requests

    private int numTransfers;
    private boolean overwrite;
    private CellStub pnfsManager;
    private CellStub pool;
    private CellStub poolManager;
    private String poolProxy;
    private String tLogRoot;

    public void shutdown() {
        executor.shutdown();
    }

    public CellStub getBilling() {
        return billing;
    }

    public ExecutorService getExecutor() {
        return executor;
    }

    public TimebasedCounter getIdGenerator() {
        return idGenerator;
    }

    public <T> void persist(T data) {
        dao.persist(data);
    }

    public void removeActiveTransfer(long id) {
        TransferManagerHandler handler = activeTransfers.remove(id);
        dao.removeHandler(handler);
    }

    public void addActiveTransfer(long id, TransferManagerHandler handler) {
        activeTransfers.put(id, handler);
        dao.addHandler(handler);
    }

//    @Override
//    public void getInfo(PrintWriter pw) {
//        pw.printf("    %s\n", getClass().getName());
//        pw.println("---------------------------------");
//        pw.printf("Name   : %s\n", getCellName());
//        if (doDbLogging()) {
//            pw.println("dblogging=true");
//        } else {
//            pw.println("dblogging=false");
//        }
//        if (idGenerator != null) {
//            pw.println("TransferID is generated using Data Base");
//        } else {
//            pw.println("TransferID is generated w/o DB access");
//        }
//        pw.printf("number of active transfers : %d\n", numTransfers);
//        pw.printf("max number of active transfers  : %d\n", getMaxTransfers());
//        pw.printf("PoolManager  : %s\n", poolManager);
//        pw.printf("PoolManager timeout : %d seconds\n",
//                        MILLISECONDS.toSeconds(poolManager.getTimeoutInMillis()));
//        pw.printf("PnfsManager timeout : %d seconds\n",
//                        MILLISECONDS.toSeconds(pnfsManager.getTimeoutInMillis()));
//        pw.printf("Pool timeout  : %d seconds\n",
//                        MILLISECONDS.toSeconds(pool.getTimeoutInMillis()));
//        pw.printf("next id  : %d seconds\n", nextMessageID);
//        pw.printf("io-queue  : %s\n", ioQueueName);
//        pw.printf("maxNumberofDeleteRetries  : %d\n", maxNumberOfDeleteRetries);
//        pw.printf("Pool Proxy : %s\n", (poolProxy == null ? "not set"
//                        : ("set to " + poolProxy)));
//    }

    public String getIoQueueName() {
        return ioQueueName;
    }

    public Set<PnfsId> getJustRequestedIDs() {
        return justRequestedIDs;
    }

    public String getLogRootName() {
        return tLogRoot;
    }

    public int getMaxNumberOfDeleteRetries() {
        return maxNumberOfDeleteRetries;
    }

    public int getMaxTransfers() {
        return maxTransfers;
    }

    public long getMoverTimeout() {
        return moverTimeout;
    }

    public Timer getMoverTimeoutTimer() {
        return moverTimeoutTimer;
    }

    public Map<Long, TimerTask> getMoverTimeoutTimerTasks() {
        return moverTimeoutTimerTasks;
    }

    public synchronized long getNextMessageID() {
        if (idGenerator != null) {
            try {
                nextMessageID = idGenerator.next();
            } catch (Exception e) {
                log.error("Having trouble getting getNextMessageID from DB");
                log.error(e.toString());
                log.error("will nullify requestsPropertyStorage");
                idGenerator = null;
                getNextMessageID();
            }
        } else {
            if (nextMessageID == Long.MAX_VALUE) {
                nextMessageID = 0;
                return Long.MAX_VALUE;
            }
            return nextMessageID++;
        }
        return nextMessageID;
    }

    public int getNumTransfers() {
        return numTransfers;
    }

    public CellStub getPnfsManager() {
        return pnfsManager;
    }

    public CellStub getPool() {
        return pool;
    }

    public CellStub getPoolManager() {
        return poolManager;
    }

    public String getPoolProxy() {
        return poolProxy;
    }

    public String gettLogRoot() {
        return tLogRoot;
    }

    public boolean isDoDatabaseLogging() {
        return doDatabaseLogging;
    }

    public boolean isOverwrite() {
        return overwrite;
    }

    public CancelTransferMessage messageArrived(CellMessage envelope,
                    CancelTransferMessage message) {
        long id = message.getId();
        TransferManagerHandler h = getHandler(id);
        if (h != null) {
            String explanation = message.getExplanation();
            h.cancel(explanation != null ? explanation
                            : "at the request of door");
        } else {
            // FIXME: shouldn't this throw an exception?
            log.error("cannot find handler with id={} for CancelTransferMessage",
                            id);
        }
        return message;
    }

    public void messageArrived(CellMessage envelope,
                    DoorTransferFinishedMessage message) {
        long id = message.getId();
        TransferManagerHandler h = getHandler(id);
        if (h != null) {
            h.poolDoorMessageArrived(message);
        } else {
            log.error("cannot find handler with id={} for DoorTransferFinishedMessage",
                            id);
        }
    }

    public TransferManagerMessage messageArrived(CellMessage envelope,
                    TransferManagerMessage message) throws CacheException {
        if (!newTransfer()) {
            throw new CacheException(TransferManagerMessage.TOO_MANY_TRANSFERS,
                            "too many transfers!");
        }
        new TransferManagerHandler(this, dao, message,
                        envelope.getSourcePath().revert(), executor).handle();
        return message;
    }

    public Object messageArrived(CellMessage envelope,
                    TransferStatusQueryMessage message) {
        TransferManagerHandler handler = getHandler(message.getId());

        if (handler == null) {
            message.setState(TransferManagerHandler.UNKNOWN_ID);
            return message;
        }

        return handler.appendInfo(message);
    }

    public void setBilling(CellStub billing) {
        this.billing = billing;
    }

    public void setDoDatabaseLogging(boolean doDatabaseLogging) {
        this.doDatabaseLogging = doDatabaseLogging;
    }

    public void setIoQueueName(String ioQueueName) {
        this.ioQueueName = ioQueueName;
    }

    public void setMaxNumberOfDeleteRetries(int maxNumberOfDeleteRetries) {
        this.maxNumberOfDeleteRetries = maxNumberOfDeleteRetries;
    }

    public void setMaxTransfers(int max_transfers) {
        maxTransfers = max_transfers;
    }

    public void setMoverTimeout(long moverTimeout) {
        this.moverTimeout = moverTimeout;
    }

    public void setNumTransfers(int numTransfers) {
        this.numTransfers = numTransfers;
    }

    public void setOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
    }

    public void setPnfsManager(CellStub pnfsManager) {
        this.pnfsManager = pnfsManager;
    }

    public void setPool(CellStub pool) {
        this.pool = pool;
    }

    public void setPoolManager(CellStub poolManager) {
        this.poolManager = poolManager;
    }

    public void setPoolProxy(String poolProxy) {
        this.poolProxy = poolProxy;
    }

    public void settLogRoot(String tLogRoot) {
        this.tLogRoot = tLogRoot;
    }

    public void startTimer(final long id) {
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                log.error("timer for handler " + id + " has expired, killing");
                Object o = moverTimeoutTimerTasks.remove(id);
                if (o == null) {
                    log.error("TimerTask.run(): timer task for handler Id={} not found in moverTimoutTimerTasks hashtable",
                                    id);
                    return;
                }
                TransferManagerHandler handler = getHandler(id);
                if (handler == null) {
                    log.error("TimerTask.run(): timer task for handler Id={} could not find handler !!!",
                                    id);
                    return;
                }
                handler.timeout();
            }
        };

        moverTimeoutTimerTasks.put(id, task);

        // this is very approximate
        // but we do not need hard real time
        moverTimeoutTimer.schedule(task, moverTimeout);
    }

    public void stopTimer(long id) {
        TimerTask tt = moverTimeoutTimerTasks.remove(id);
        if (tt == null) {
            log.error("stopTimer(): timer not found for Id={}", id);
            return;
        }
        log.debug("canceling the mover timer for handler id {}", id);
        tt.cancel();
    }

    protected TransferManagerHandler getHandler(long handlerId) {
        return activeTransfers.get(handlerId);
    }

    public abstract IpProtocolInfo getProtocolInfo(
                    TransferManagerMessage transferRequest);

    public void initialize() {
//        Args args = getArgs();
//        tLogRoot = Strings.emptyToNull(args.getOpt("tlog"));
//        maxNumberOfDeleteRetries = args.getIntOption("maxNumberOfDeleteRetries");
//        pool.setTimeout(args.getIntOption("pool_timeout"));
//        pool.setTimeoutUnit(TimeUnit.valueOf(args.getOpt("pool_timeout_unit")));
//        maxTransfers = args.getIntOption("max_transfers");
//        overwrite = Strings.nullToEmpty(args.getOpt("overwrite")).equalsIgnoreCase(
//                        "true");
//        poolManager.setDestination(args.getOpt("poolManager"));
//        poolManager.setTimeout(args.getIntOption("pool_manager_timeout"));
//        poolManager.setTimeoutUnit(TimeUnit.valueOf(args.getOpt("pool_manager_timeout_unit")));
//        pnfsManager.setDestination(args.getOpt("pnfsManager"));
//        pnfsManager.setTimeout(args.getIntOption("pnfs_timeout"));
//        pnfsManager.setTimeoutUnit(TimeUnit.valueOf(args.getOpt("pnfs_timeout_unit")));
//
//        moverTimeout = MILLISECONDS.convert(args.getIntOption("mover_timeout"),
//                        TimeUnit.valueOf(args.getOpt("mover_timeout_unit")));
//        ioQueueName = Strings.emptyToNull(args.getOpt("io-queue"));
//        poolProxy = args.getOpt("poolProxy");
//        log.debug("Pool Proxy "
//                        + (poolProxy == null ? "not set"
//                                        : ("set to " + poolProxy)));
    }

    public synchronized void finishTransfer() {
        log.debug("finishTransfer() num_transfers = {} DECREMENT", numTransfers);
        numTransfers--;
    }

    private synchronized boolean newTransfer() {
        log.debug("newTransfer() num_transfers = {} max_transfers={}",
                        numTransfers, maxTransfers);
        if (numTransfers == maxTransfers) {
            log.debug("newTransfer() returns false");
            return false;
        }
        log.debug("newTransfer() INCREMENT and return true");
        numTransfers++;
        return true;
    }

    public void setDao(TransferManagerDAO dao) {
        this.dao = dao;
    }

    public void setCellEndpoint(CellEndpoint endpoint) {
       this.endpoint = endpoint;
    }

    public CellEndpoint getEndpoint() {
        return endpoint;
    }
}
