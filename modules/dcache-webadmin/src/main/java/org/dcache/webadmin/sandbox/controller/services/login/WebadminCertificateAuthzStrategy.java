package org.dcache.webadmin.sandbox.controller.services.login;

import org.apache.wicket.Session;
import org.apache.wicket.authorization.strategies.page.SimplePageAuthorizationStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dcache.webadmin.sandbox.WebadminSession;
import org.dcache.webadmin.sandbox.controller.LogInService;
import org.dcache.webadmin.sandbox.controller.beans.UserBean;
import org.dcache.webadmin.sandbox.controller.exceptions.LogInServiceException;
import org.dcache.webadmin.sandbox.view.AuthenticatedWebPage;
import org.dcache.webadmin.sandbox.view.pages.login.LogIn;

public class WebadminCertificateAuthzStrategy extends SimplePageAuthorizationStrategy {
    private static final Logger LOGGER
        = LoggerFactory.getLogger(WebadminCertificateAuthzStrategy.class);

    private LogInService loginService;

    public WebadminCertificateAuthzStrategy() {
        super(AuthenticatedWebPage.class, LogIn.class);
    }

    @Override
    protected boolean isAuthorized() {
        boolean signedIn = ((WebadminSession) Session.get()).isSignedIn();
        if (!signedIn) {
            try {
                /*
                 * passwordless, i.e., certificate-based
                 */
                UserBean user = loginService.authenticate();
                WebadminSession session = (WebadminSession) Session.get();
                session.setUser(user);
                signedIn = true;
            } catch (IllegalArgumentException | LogInServiceException e) {
                LOGGER.debug("could not automatically authorize "
                                + "using browser certificate: {}",
                                e.toString());
            }
        }
        return signedIn;
    }

    public void setLoginService(LogInService loginService) {
        this.loginService = loginService;
    }
}
