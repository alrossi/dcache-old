package diskCacheV111.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.dcache.alarms.AlarmMarkerFactory;
import org.dcache.alarms.Severity;

/**
 * Utility class for invoking an HSM integration script.
 */
public class HsmRunSystem extends RunSystem
{
    private static final Logger LOGGER = LoggerFactory.getLogger(HsmRunSystem.class);
    private static InetAddress host;

    static {
        try {
            host = InetAddress.getLocalHost();
        } catch (UnknownHostException t) {
            LOGGER.error("Unable to determine host address: {}", t.getMessage());
        }
    }

    public HsmRunSystem(String exec, int maxLines, long timeout)
    {
        super(exec, maxLines, timeout);
    }

    public String execute() throws IOException, CacheException
    {
        go();
        int returnCode = getExitValue();
        try {
            switch (returnCode) {
                case 0:
                    break;
                case 71:
                    throw new CacheException(CacheException.HSM_DELAY_ERROR,
                                    "HSM script failed (script reported 71: "
                                                    + getErrorString() + ")");
                case 143:
                    throw new TimeoutCacheException(
                                    "HSM script was killed (script reported 143: "
                                                    + getErrorString() + ")");
                default:
                    throw new CacheException(returnCode,
                                    "HSM script failed (script reported: "
                                                    + returnCode + ": "
                                                    + getErrorString());
            }
        } catch (CacheException e) {
            if (host != null) {
                LOGGER.error(AlarmMarkerFactory.getMarker(Severity.HIGH,
                                                          "HSM_SCRIPT_FAILURE",
                                                          host.getCanonicalHostName()),
                                    e.getMessage());
            } else {
                LOGGER.error(AlarmMarkerFactory.getMarker(Severity.HIGH,
                                                          "HSM_SCRIPT_FAILURE"),
                                    e.getMessage());
            }
            throw e;
        }
        return getOutputString();
    }
}
