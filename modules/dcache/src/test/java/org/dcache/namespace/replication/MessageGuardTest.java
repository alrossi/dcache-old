package org.dcache.namespace.replication;

import org.junit.Test;

import java.util.UUID;

import dmg.cells.nucleus.CDC;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.CellPath;

import static org.mockito.BDDMockito.given;
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
        given(msg.getSourcePath()).willReturn(new CellPath("Foo", "bar"));

        handler.messageArrived(msg);

        assert(!guard.acceptMessage("test", msg));
    }

    @Test
    public void shouldAcceptMessage() throws Exception {
        given(msg.getSession()).willReturn(UUID.randomUUID().toString());
        given(msg.getSourcePath()).willReturn(new CellPath("Foo", "bar"));

        handler.messageArrived(msg);

        assert(guard.acceptMessage("test", msg));
    }
}
