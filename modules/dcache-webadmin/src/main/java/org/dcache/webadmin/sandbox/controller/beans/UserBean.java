package org.dcache.webadmin.sandbox.controller.beans;

import org.apache.wicket.authroles.authorization.strategies.role.Roles;

import java.io.Serializable;

public class UserBean implements Serializable {
    private static final long serialVersionUID = -266376958298121232L;

    private String name;
    private Roles roles = new Roles();

    public void setRoles(Roles roles) {
        this.roles = roles;
    }

    public void addRole(String role) {
        roles.add(role);
    }

    public String getUsername() {
        return name;
    }

    public void setUsername(String name) {
        this.name = name;
    }

    public boolean hasAnyRole(Roles roles) {
        return this.roles.hasAnyRole(roles);
    }
}
