package org.dcache.webadmin.view.beans;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author jans
 */
public class PoolQueueBean implements Comparable, Serializable {

    private String _name = "";
    private String _domainName = "";
    private Map<String, PoolRequestQueue> _movers =
            new HashMap<String, PoolRequestQueue>();
    private static final Logger _log = LoggerFactory.getLogger(PoolQueueBean.class);

    public PoolQueueBean() {
    }

    public String getDomainName() {
        return _domainName;
    }

    public void setDomainName(String domainName) {
        _domainName = domainName;
    }

    public String getName() {
        return _name;
    }

    public void setName(String name) {
        _name = name;
    }

    public Map<String, PoolRequestQueue> getMoverQueues() {
        return _movers;
    }

    public void setMoverQueues(HashMap<String, PoolRequestQueue> moverQueues) {
        _movers = moverQueues;
    }

    public void addRequestQueue(PoolRequestQueue queue) {
        _movers.put(queue.getName(), queue);
    }

    public int compareTo(Object other) {

        if (other == null) {
            throw new NullPointerException();
        }
//      throws ClassCastException if wrong object is delivered, according to
//      specification
        PoolQueueBean otherBean = (PoolQueueBean) other;
        return this.getName().compareTo(otherBean.getName());
    }
}
