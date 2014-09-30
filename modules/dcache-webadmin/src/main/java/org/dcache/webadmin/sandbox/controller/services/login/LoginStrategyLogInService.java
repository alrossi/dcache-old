package org.dcache.webadmin.sandbox.controller.services.login;

import org.apache.wicket.protocol.http.servlet.ServletWebRequest;
import org.apache.wicket.request.cycle.RequestCycle;
import org.apache.wicket.authroles.authorization.strategies.role.Roles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.servlet.http.HttpServletRequest;

import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;

import diskCacheV111.util.CacheException;

import org.dcache.auth.LoginReply;
import org.dcache.auth.LoginStrategy;
import org.dcache.auth.PasswordCredential;
import org.dcache.auth.Subjects;
import org.dcache.util.CertificateFactories;
import org.dcache.webadmin.sandbox.controller.LogInService;
import org.dcache.webadmin.sandbox.controller.beans.UserBean;
import org.dcache.webadmin.sandbox.controller.exceptions.LogInServiceException;

import static java.util.Arrays.asList;

public final class LoginStrategyLogInService implements LogInService {
    private static final Logger LOGGER = LoggerFactory.getLogger(LogInService.class);
    private static final String X509_CERTIFICATE_ATTRIBUTE
        = "javax.servlet.request.X509Certificate";

    private static X509Certificate[] getCertChain() {
        ServletWebRequest servletWebRequest
            = (ServletWebRequest) RequestCycle.get().getRequest();
        HttpServletRequest request = servletWebRequest.getContainerRequest();
        Object certificate = request.getAttribute(X509_CERTIFICATE_ATTRIBUTE);
        X509Certificate[] chain;
        if (certificate instanceof X509Certificate[]) {
            chain = (X509Certificate[]) certificate;
        } else {
            throw new IllegalArgumentException();
        }
        return chain;
    }

    private final CertificateFactory certificateFactory;
    private LoginStrategy loginStrategy;
    private int adminGid;

    public LoginStrategyLogInService() {
        certificateFactory = CertificateFactories.newX509CertificateFactory();
    }

    @Override
    public UserBean authenticate() throws LogInServiceException {
        try {
            Subject subject = new Subject();
            X509Certificate[] certChain = getCertChain();
            subject.getPublicCredentials().add(
                            certificateFactory.generateCertPath(asList(certChain)));
            return authenticate(subject);
        } catch (CertificateException e) {
            throw new LogInServiceException(
                            "Failed to generate X.509 certificate path: "
                                            + e.getMessage(), e);
        }
    }

    @Override
    public UserBean authenticate(String username, char[] password)
                    throws LogInServiceException {
        Subject subject = new Subject();
        PasswordCredential pass = new PasswordCredential(username,
                        String.valueOf(password));
        subject.getPrivateCredentials().add(pass);
        return authenticate(subject);
    }

    public void setAdminGid(int adminGid) {
        LOGGER.debug("admin GID set to {}", adminGid);
        this.adminGid = adminGid;
    }

    public void setLoginStrategy(LoginStrategy loginStrategy) {
        if (loginStrategy == null) {
            throw new IllegalArgumentException();
        }
        this.loginStrategy = loginStrategy;
    }

    private UserBean authenticate(Subject subject) throws LogInServiceException {
        LoginReply login;
        try {
            login = loginStrategy.login(subject);
            if (login == null) {
                throw new NullPointerException();
            }
        } catch (CacheException ex) {
            throw new LogInServiceException(ex.getMessage(), ex);
        }
        UserBean user = mapLoginToUser(login);
        return user;
    }

    private Roles mapGidsToRoles(long[] gids) {
        Roles roles = new Roles();
        boolean isAdmin = false;
        for (long gid : gids) {
            LOGGER.debug("GID : {}", gid);
            if (gid == adminGid) {
                roles.add(Role.ADMIN.toString());
                isAdmin = true;
            }
        }
        if (!isAdmin) {
            roles.add(Role.USER.toString());
        }
        return roles;
    }

    private UserBean mapLoginToUser(LoginReply login) {
        UserBean user = new UserBean();
        Subject subject = login.getSubject();
        user.setUsername(Subjects.getUserName(subject));
        Roles roles = mapGidsToRoles(Subjects.getGids(subject));
        user.setRoles(roles);
        return user;
    }
}
