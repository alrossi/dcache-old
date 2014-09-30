package org.dcache.webadmin.sandbox.controller.beans;

import java.io.Serializable;

public class LogInBean implements Serializable {
    private static final long serialVersionUID = -4692746509263501015L;

    private String username = "";
    private String password = "";
    private boolean remembering = true;

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public boolean isRemembering() {
        return remembering;
    }

    public void setRemembering(boolean rememberMe) {
        this.remembering = rememberMe;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }
}
