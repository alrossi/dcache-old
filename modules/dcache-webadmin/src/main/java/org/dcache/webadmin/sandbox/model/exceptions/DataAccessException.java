package org.dcache.webadmin.sandbox.model.exceptions;

public class DataAccessException extends Exception {

    private static final long serialVersionUID = -1276108376482381265L;

    public DataAccessException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public DataAccessException(Throwable cause) {
        super(cause);
    }

    public DataAccessException(String msg) {
        super(msg);
    }
}
