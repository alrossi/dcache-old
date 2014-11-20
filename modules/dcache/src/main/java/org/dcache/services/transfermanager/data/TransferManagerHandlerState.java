package org.dcache.services.transfermanager.data;

import java.io.Serializable;

/**
 * @author arossi
 *
 */
public class TransferManagerHandlerState {
    protected int state;
    protected long transferid;
    protected long transitionTime;
    protected boolean causedByError;
    protected String description;

    protected TransferManagerHandlerState() {
    }

    public TransferManagerHandlerState(TransferManagerHandler handler,
                    Serializable errorObject) {

        transferid = handler.getId();
        state = handler.getState();
        transitionTime = System.currentTimeMillis();
        switch (state) {
            case TransferManagerHandler.INITIAL_STATE:
                description = "INITIAL_STATE";
                causedByError = false;
                break;
            case TransferManagerHandler.WAITING_FOR_PNFS_INFO_STATE:
                description = "WAITING_FOR_PNFS_INFO_STATE";
                causedByError = false;
                break;
            case TransferManagerHandler.RECEIVED_PNFS_INFO_STATE:
                description = "RECEIVED_PNFS_INFO_STATE";
                causedByError = false;
                break;
            case TransferManagerHandler.WAITING_FOR_PNFS_ENTRY_CREATION_INFO_STATE:
                description = "WAITING_FOR_PNFS_ENTRY_CREATION_INFO_STATE";
                causedByError = false;
                break;
            case TransferManagerHandler.RECEIVED_PNFS_ENTRY_CREATION_INFO_STATE:
                description = "RECEIVED_PNFS_ENTRY_CREATION_INFO_STATE";
                causedByError = false;
                break;
            case TransferManagerHandler.WAITING_FOR_POOL_INFO_STATE:
                description = "WAITING_FOR_POOL_INFO_STATE";
                causedByError = false;
                break;
            case TransferManagerHandler.RECEIVED_POOL_INFO_STATE:
                description = "RECEIVED_POOL_INFO_STATE";
                causedByError = false;
                break;
            case TransferManagerHandler.WAITING_FIRST_POOL_REPLY_STATE:
                description = "WAITING_FIRST_POOL_REPLY_STATE";
                causedByError = false;
                break;
            case TransferManagerHandler.RECEIVED_FIRST_POOL_REPLY_STATE:
                description = "RECEIVED_FIRST_POOL_REPLY_STATE";
                causedByError = false;
                break;
            case TransferManagerHandler.WAITING_FOR_SPACE_INFO_STATE:
                description = "WAITING_FOR_SPACE_INFO_STATE";
                causedByError = false;
                break;
            case TransferManagerHandler.RECEIVED_SPACE_INFO_STATE:
                description = "RECEIVED_SPACE_INFO_STATE";
                causedByError = false;
                break;
            case TransferManagerHandler.SENT_ERROR_REPLY_STATE:
                description = "SENT_ERROR_REPLY_STATE";
                if (errorObject != null) {
                    description += "(" + errorObject + ")";
                }
                causedByError = true;
                break;
            case TransferManagerHandler.WAITING_FOR_PNFS_ENTRY_DELETE:
                description = "WAITING_FOR_PNFS_ENTRY_DELETE";
                causedByError = true;
                break;
            case TransferManagerHandler.RECEIVED_PNFS_ENTRY_DELETE:
                description = "RECEIVED_PNFS_ENTRY_DELETE";
                causedByError = true;
                break;
            case TransferManagerHandler.SENT_SUCCESS_REPLY_STATE:
                description = "SENT_SUCCESS_REPLY_STATE";
                causedByError = false;
                break;
            default:
                description = "";
                causedByError = false;
                break;
        }
    }
}
