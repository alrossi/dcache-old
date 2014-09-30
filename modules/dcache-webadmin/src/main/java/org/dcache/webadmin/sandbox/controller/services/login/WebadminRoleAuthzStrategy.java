package org.dcache.webadmin.sandbox.controller.services.login;

import org.apache.wicket.Session;
import org.apache.wicket.authroles.authorization.strategies.role.IRoleCheckingStrategy;
import org.apache.wicket.authroles.authorization.strategies.role.RoleAuthorizationStrategy;
import org.apache.wicket.authroles.authorization.strategies.role.Roles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dcache.webadmin.sandbox.WebadminSession;

public class WebadminRoleAuthzStrategy extends RoleAuthorizationStrategy {
    private static final Logger LOGGER = LoggerFactory.getLogger(WebadminRoleAuthzStrategy.class);

    public WebadminRoleAuthzStrategy() {
        super(new IRoleCheckingStrategy() {
            @Override
            public boolean hasAnyRole(Roles roles) {
                LOGGER.debug("checking {}", roles.toString());
                boolean hasAnyRoles = ((WebadminSession) Session.get()).hasAnyRole(roles);
                LOGGER.debug("results in: {}", hasAnyRoles);
                return hasAnyRoles;
            }
        });
    }
}
