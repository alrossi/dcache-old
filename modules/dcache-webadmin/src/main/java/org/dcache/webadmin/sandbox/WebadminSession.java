package org.dcache.webadmin.sandbox;

import org.apache.wicket.Session;
import org.apache.wicket.authroles.authorization.strategies.role.Roles;
import org.apache.wicket.protocol.http.WebSession;
import org.apache.wicket.request.Request;

import org.dcache.webadmin.sandbox.controller.beans.UserBean;

public class WebadminSession extends WebSession {
    private static final long serialVersionUID = -941613160805323716L;

    private UserBean user;

    public static WebadminSession get() {
        return (WebadminSession) Session.get();
    }

    public WebadminSession(Request request) {
        super(request);
    }

    public String getUserName() {
        return user.getUsername();
    }

    public boolean hasAnyRole(Roles roles) {
        if (!isSignedIn()) {
            return false;
        }
        return user.hasAnyRole(roles);
    }

    public boolean isSignedIn() {
        return user != null;
    }

    public void logoutUser() {
        user = null;
    }

    public void setUser(final UserBean user) {
        this.user = user;
    }
}
