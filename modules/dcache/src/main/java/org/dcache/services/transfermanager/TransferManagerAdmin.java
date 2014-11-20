package org.dcache.services.transfermanager;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.dcache.services.transfermanager.data.TransferManagerHandler;
import org.dcache.util.Args;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * @author arossi
 *
 */
public class TransferManagerAdmin {
    private TransferManager manager;





    public final static String hh_set_dblogging = "<true/false switch db loggin on/off>";

    public String ac_set_dblogging_$_1(Args args)
    {
        String logString = args.argv(0);
        StringBuilder sb = new StringBuilder();
        if (logString.equalsIgnoreCase("true") || logString.equalsIgnoreCase("t")) {
            setDbLogging(true);
            sb.append("remote ftp transaction db logging is on\n");
        } else if (logString.equalsIgnoreCase("false") || logString.equalsIgnoreCase("f")) {
            setDbLogging(false);
            sb.append("remote ftp transaction db logging is off\n");
        } else {
            return "unrecognized value : \"" + logString + "\" only true or false are allowed";
        }
        if (doDbLogging() == true && _pm == null) {
            sb.append(getCellName()).append(" has been started w/ db logging disabled\n");
            sb.append("Attempting to initialize JDO Persistence Manager using parameters provided at startup\n");
            try {
                _pm = createPersistenceManager();
                sb.append("Success...\n");
            } catch (Exception e) {
                log.error(e.toString());
                sb.append("Failure...\n");
                sb.append("setting doDbLog back to false. \n");
                sb.append("Try to set correct Jdbc driver, username or password for DB connection.\n");
                _pm = null;
                setDbLogging(false);
            }
        }
        return sb.toString();
    }

    public String ac_set_maxNumberOfDeleteRetries_$_1(Args args)
    {
        _maxNumberOfDeleteRetries = Integer.parseInt(args.argv(0));
        return "setting maxNumberOfDeleteRetries " + _maxNumberOfDeleteRetries;
    }

    public final static String hh_set_tlog = "<direcory for ftp logs or \"null\" for none>";

    public String ac_set_tlog_$_1(Args args)
    {
        _tLogRoot = args.argv(0);
        if (_tLogRoot.equals("null")) {
            _tLogRoot = null;
            return "remote ftp transaction logging is off";
        }
        return "remote ftp transactions will be logged to " + _tLogRoot;
    }

    public final static String hh_set_max_transfers = "<#max transfers>";

    public String ac_set_max_transfers_$_1(Args args)
    {
        int max = Integer.parseInt(args.argv(0));
        if (max <= 0) {
            return "Error, max transfers number should be greater then 0 ";
        }
        setMaxTransfers(max);
        return "set maximum number of active transfers to " + max;
    }

    public final static String hh_set_pool_timeout = "<#seconds>";

    public String ac_set_pool_timeout_$_1(Args args)
    {
        int timeout = Integer.parseInt(args.argv(0));
        if (timeout <= 0) {
            return "Error, pool timeout should be greater then 0 ";
        }
        _poolStub.setTimeout(timeout);
        _poolStub.setTimeoutUnit(SECONDS);
        return "set pool timeout to " + timeout + " seconds";
    }

    public final static String hh_set_pool_manager_timeout = "<#seconds>";

    public String ac_set_pool_manager_timeout_$_1(Args args)
    {
        int timeout = Integer.parseInt(args.argv(0));
        if (timeout <= 0) {
            return "Error, pool manger timeout should be greater then 0 ";
        }
        _poolManager.setTimeout(timeout);
        _poolManager.setTimeoutUnit(SECONDS);
        return "set pool manager timeout to " + timeout + " seconds";
    }

    public final static String hh_set_pnfs_manager_timeout = "<#seconds>";

    public String ac_set_pnfs_manager_timeout_$_1(Args args)
    {
        int timeout = Integer.parseInt(args.argv(0));
        if (timeout <= 0) {
            return "Error, pnfs manger timeout should be greater then 0 ";
        }
        _pnfsManager.setTimeout(timeout);
        _pnfsManager.setTimeoutUnit(SECONDS);
        return "set pnfs manager timeout to " + timeout + " seconds";
    }

    public final static String hh_ls = "[-l] [<#transferId>]";

    public String ac_ls_$_0_1(Args args)
    {
        boolean long_format = args.hasOption("l");
        if (args.argc() > 0) {
            long id = Long.parseLong(args.argv(0));
            TransferManagerHandler handler = _activeTransfers.get(id);
            if (handler == null) {
                return "ID not found : " + id;
            }
            return " transfer id=" + id + " : " + handler.toString(long_format);
        }
        StringBuilder sb = new StringBuilder();
        if (_activeTransfers.isEmpty()) {
            return "No Active Transfers";
        }
        sb.append("  Active Transfers ");
        for (Map.Entry<Long, TransferManagerHandler> e : _activeTransfers.entrySet()) {
            sb.append("\n#").append(e.getKey());
            sb.append(" ").append(e.getValue().toString(long_format));
        }
        return sb.toString();
    }

    public final static String hh_kill = " id";

    public String ac_kill_$_1(Args args)
    {
        long id = Long.parseLong(args.argv(0));
        TransferManagerHandler handler = _activeTransfers.get(id);
        if (handler == null) {
            return "transfer not found: " + id;
        }
        handler.cancel("triggered by admin");
        return "request sent to kill the mover on pool\n";
    }

    public final static String hh_killall = " [-p pool] pattern [pool] \n"
            + " for example killall .* ketchup will kill all transfers with movers on the ketchup pool";

    public String ac_killall_$_1_2(Args args)
    {
        try {
            Pattern p = Pattern.compile(args.argv(0));
            String pool = null;
            if (args.argc() > 1) {
                pool = args.argv(1);
            }
            List<TransferManagerHandler> handlersToKill =
                    new ArrayList<>();
            for (Map.Entry<Long, TransferManagerHandler> e : _activeTransfers.entrySet()) {
                long id = e.getKey();
                TransferManagerHandler handler = e.getValue();
                Matcher m = p.matcher(String.valueOf(id));
                if (m.matches()) {
                    log.debug("pattern: \"{}\" matches id=\"{}\"", args.argv(0), id);
                    if (pool != null && pool.equals(handler.getPool())) {
                        handlersToKill.add(handler);
                    } else if (pool == null) {
                        handlersToKill.add(handler);
                    }
                } else {
                    log.debug("pattern: \"{}\" does not match id=\"{}\"", args.argv(0), id);
                }
            }
            if (handlersToKill.isEmpty()) {
                return "no active transfers match the pattern and the pool";
            }
            StringBuilder sb = new StringBuilder("Killing these transfers: \n");
            for (TransferManagerHandler handler : handlersToKill) {
                handler.cancel("triggered by admin");
                sb.append(handler.toString(true)).append('\n');
            }
            return sb.toString();
        } catch (Exception e) {
            log.error(e.toString());
            return e.toString();
        }
    }

    public final static String hh_set_io_queue = "<io-queue name >";

    public String ac_set_io_queue_$_1(Args args)
    {
        String newIoQueueName = args.argv(0);
        if (newIoQueueName.equals("null")) {
            _ioQueueName = null;
            return "io-queue is set to null";
        }
        _ioQueueName = newIoQueueName;
        return "io_queue was set to " + _ioQueueName;
    }

    public void setManager(TransferManager manager) {
        this.manager = manager;
    }
}
