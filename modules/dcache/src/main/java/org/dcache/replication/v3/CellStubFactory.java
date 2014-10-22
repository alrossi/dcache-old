package org.dcache.replication.v3;

import java.util.concurrent.TimeUnit;

import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessageSender;

import org.dcache.cells.CellStub;

/**
 * @author arossi
 */
public class CellStubFactory implements CellMessageSender {
    private CellEndpoint endpoint;
    private Long messageTimeout;
    private TimeUnit messageTimeoutUnit;

    public CellStub getCellStub(String destination) {
        CellStub stub = new CellStub();
        stub.setDestination(destination);
        stub.setCellEndpoint(endpoint);
        if (messageTimeout != null) {
            stub.setTimeout(messageTimeout);
        }
        if (messageTimeoutUnit != null) {
            stub.setTimeoutUnit(messageTimeoutUnit);
        }
        return stub;
    }

    public void setCellEndpoint(CellEndpoint endpoint) {
        this.endpoint = endpoint;
    }

    public void setMessageTimeout(long messageTimeout) {
        this.messageTimeout = messageTimeout;
    }

    public void setMessageTimeoutUnit(TimeUnit messageTimeoutUnit) {
        this.messageTimeoutUnit = messageTimeoutUnit;
    }
}
