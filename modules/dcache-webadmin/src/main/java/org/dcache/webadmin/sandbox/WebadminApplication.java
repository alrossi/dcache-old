package org.dcache.webadmin.sandbox;

import com.google.common.base.Throwables;
import org.apache.wicket.Page;
import org.apache.wicket.Session;
import org.apache.wicket.authorization.IAuthorizationStrategy;
import org.apache.wicket.authorization.strategies.CompoundAuthorizationStrategy;
import org.apache.wicket.core.request.mapper.CryptoMapper;
import org.apache.wicket.protocol.http.WebApplication;
import org.apache.wicket.protocol.https.HttpsConfig;
import org.apache.wicket.protocol.https.HttpsMapper;
import org.apache.wicket.request.Request;
import org.apache.wicket.request.Response;
import org.springframework.beans.factory.annotation.Required;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.dcache.webadmin.sandbox.controller.DataProvider;
import org.dcache.webadmin.sandbox.controller.DataService;
import org.dcache.webadmin.sandbox.controller.LogInService;
import org.dcache.webadmin.sandbox.controller.services.NOPService;
import org.dcache.webadmin.sandbox.controller.services.login.Role;
import org.dcache.webadmin.sandbox.view.BaseWebadminPage;
import org.dcache.webadmin.sandbox.view.pages.home.Home;
import org.dcache.webadmin.sandbox.view.pages.login.LogIn;
import org.dcache.webadmin.sandbox.view.panels.navigation.NavigationPanel;

import static org.apache.wicket.authroles.authorization.strategies.role.metadata.MetaDataRoleAuthorizationStrategy.authorize;

public class WebadminApplication extends WebApplication {
    private Map<String, String> paths;
    private Map<String, String> linkNames;
    private Map<String, DataService> services;
    private Set<String> adminPages;
    private List<IAuthorizationStrategy> authzStrategies;
    private LogInService loginService;
    private String dcacheName;
    private int httpsPort;
    private int httpPort;

    public String getDcacheName() {
        return dcacheName;
    }

    @Override
    public Class<? extends Page> getHomePage() {
        return Home.class;
    }

    public LogInService getLoginService() {
        return loginService;
    }

    public boolean isAdminPage(Class page) {
        return adminPages != null &&
               adminPages.contains(page.getName());
    }

    public boolean isAuthenticated() {
        return loginService != null;
    }

    @Override
    public Session newSession(Request request, Response response) {
        return new WebadminSession(request);
    }

    public synchronized void refreshPageData(Class page, DataProvider provider) {
        DataService service = services.get(page.getName());
        if (service != null) {
            service.refresh(provider);
        }
    }

    public void setAdminPages(Set<String> adminPages) {
        this.adminPages = adminPages;
    }

    public void setAuthzStrategies(List<IAuthorizationStrategy> authzStrategies) {
        this.authzStrategies = authzStrategies;
    }

    @Required
    public void setDcacheName(String dCacheName) {
        this.dcacheName = dCacheName;
    }

    @Required
    public void setHttpPort(int port) {
        this.httpPort = port;
    }

    public void setHttpsPort(int port) {
        this.httpsPort = port;
    }

    @Required
    public void setLinkNames(Map<String, String> linkNames) {
        this.linkNames = linkNames;
    }

    public void setLoginService(LogInService loginService) {
        this.loginService = loginService;
    }

    @Required
    public void setPaths(Map<String, String> paths) {
        this.paths = paths;
    }

    @Required
    public void setServices(Map<String, DataService> services) {
        this.services = services;
    }

    public void shutdown() {
        for (DataService service: services.values()) {
            service.shutdown();
        }
    }

    @Override
    protected void init() {
        super.init();

        try {
            initializePages();
        } catch (ClassNotFoundException t) {
            Throwables.propagate(t);
        }

        setRootRequestMapper(new CryptoMapper(getRootRequestMapper(), this));
        if (isAuthenticated()) {
            mountPage("login", LogIn.class);
            getApplicationSettings().setPageExpiredErrorPage(LogIn.class);
            setAuthorizationStrategies();
            setRootRequestMapper(new HttpsMapper(getRootRequestMapper(),
                                 new HttpsConfig(httpPort, httpsPort)));
        } else {
            getApplicationSettings().setPageExpiredErrorPage(Home.class);
        }
    }

    private void initializePages() throws ClassNotFoundException {
        for (String clzz: paths.keySet()) {
            DataService service = services.get(clzz);
            if (service instanceof NOPService) {
                continue;
            }

            Class<? extends BaseWebadminPage> page
                = (Class<? extends BaseWebadminPage>)
                    Thread.currentThread().getContextClassLoader()
                                          .loadClass(clzz);
            if (adminPages.contains(clzz)) {
                authorize(page, Role.ADMIN.toString());
            }

            mountPage(paths.get(clzz), page);
            NavigationPanel.addLink(linkNames.get(clzz), page);

            if (service != null) {
                service.initialize();
            }
        }
    }

    private void setAuthorizationStrategies() {
        CompoundAuthorizationStrategy compoundStrategy
            = new CompoundAuthorizationStrategy();
        for (IAuthorizationStrategy strategy: authzStrategies) {
            compoundStrategy.add(strategy);
        }
        getSecuritySettings().setAuthorizationStrategy(compoundStrategy);
    }
}
