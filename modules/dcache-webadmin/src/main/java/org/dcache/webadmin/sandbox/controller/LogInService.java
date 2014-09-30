package org.dcache.webadmin.sandbox.controller;

import org.dcache.webadmin.sandbox.controller.beans.UserBean;
import org.dcache.webadmin.sandbox.controller.exceptions.LogInServiceException;

public interface LogInService {
    UserBean authenticate(String username, char[] password)
                    throws LogInServiceException;

    UserBean authenticate() throws LogInServiceException;
}
