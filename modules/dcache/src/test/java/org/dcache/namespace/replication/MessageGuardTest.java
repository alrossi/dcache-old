package org.dcache.namespace.replication;

import org.junit.Test;

import java.util.UUID;

import dmg.cells.nucleus.CDC;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageReceiver;

import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;

/**
 * Created by arossi on 2/10/15.
 */
public class MessageGuardTest {

    class MessageReceiver implements CellMessageReceiver {
        void messageArrived(CellMessage msg) {
            CDC.setMessageContext(msg);
        }
    }

    MessageReceiver handler = new MessageReceiver();
    MessageGuard guard = new MessageGuard();

    CellMessage msg = mock(CellMessage.class);

    @Test
    public void shouldRejectMessage() throws Exception {
        given(msg.getSession()).willReturn(MessageGuard.REPLICA_ID);

        handler.messageArrived(msg);

        assert(!guard.acceptMessage(anyString(), msg));
    }

    @Test
    public void shouldAcceptMessage() throws Exception {
        given(msg.getSession()).willReturn(UUID.randomUUID().toString());

        handler.messageArrived(msg);

        assert(guard.acceptMessage(anyString(), msg));
    }
}
