package org.dcache.webadmin.sandbox.view;

import org.apache.wicket.markup.head.IHeaderResponse;
import org.apache.wicket.markup.head.JavaScriptHeaderItem;
import org.apache.wicket.markup.head.StringHeaderItem;
import org.apache.wicket.markup.html.WebPage;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.panel.FeedbackPanel;
import org.apache.wicket.model.ResourceModel;
import org.apache.wicket.model.StringResourceModel;
import org.apache.wicket.protocol.http.servlet.ServletWebRequest;
import org.apache.wicket.request.mapper.parameter.PageParameters;

import java.util.MissingResourceException;
import java.util.concurrent.TimeUnit;

import org.dcache.webadmin.sandbox.WebadminApplication;
import org.dcache.webadmin.sandbox.WebadminSession;
import org.dcache.webadmin.sandbox.controller.DataProvider;
import org.dcache.webadmin.sandbox.view.panels.navigation.NavigationPanel;
import org.dcache.webadmin.sandbox.view.panels.user.UserPanel;
import org.dcache.webadmin.view.panels.header.HeaderPanel;


public abstract class BaseWebadminPage<P extends DataProvider> extends WebPage {
    private static final long serialVersionUID = 7817347486820155316L;

    private static final String MISSING_RESOURCE_KEY = "missing.resource";
    private static final String JS_MODULES = "js_modules";
    private static final String WICKET_HEADER_OPEN = "<!-- wicket ";
    private static final String WICKET_HEADER_START = " header BEGIN -->\n";
    private static final String WICKET_HEADER_END =  " header END -->\n";

    protected P provider;

    protected BaseWebadminPage() {
        initialize();
    }

    protected BaseWebadminPage(PageParameters parameters) {
        super(parameters);
        initialize();
    }

    public WebadminApplication getWebadminApplication() {
        return (WebadminApplication) getApplication();
    }

    public WebadminSession getWebadminSession() {
        return (WebadminSession) getSession();
    }

    public void refresh() {
        getWebadminApplication().refreshPageData(this.getClass(), provider);
    }

    @Override
    public void renderHead(IHeaderResponse response) {
        super.renderHead(response);
        response.render(new StringHeaderItem(WICKET_HEADER_OPEN
                                             + this.getClass().getSimpleName()
                                             + WICKET_HEADER_START));
        renderHeadInternal(response);
        response.render(new StringHeaderItem(WICKET_HEADER_OPEN
                                             + this.getClass().getSimpleName()
                                             + WICKET_HEADER_END));
    }

    protected String getStringResource(String resourceKey) {
        try {
            return new StringResourceModel(resourceKey, this, null).getString();
        } catch (MissingResourceException e) {
        }
        return getString(MISSING_RESOURCE_KEY);
    }

    protected void initialize() {
        initializeProvider();
        setTimeout();
        add(new Label("pageTitle", new ResourceModel("title")));
        add(new HeaderPanel("headerPanel"));
        /*
         * Conditional markup.  We need to fix this with fragments or something TODO
         */
        if (getWebadminApplication().isAuthenticated()) {
            add(new UserPanel("userPanel", this));
        }
        NavigationPanel navigation = new NavigationPanel("navigationPanel",
                                                         this.getClass());
        add(navigation);
        add(new FeedbackPanel("feedback"));
    }

    protected abstract void initializeProvider();

    /*
     *  Loads all the relevant Javascript/JQuery functions.
     *  For a clean separation, no javascript should appear
     *  in Java code, but should be modularized and placed
     *  in the "js" subdirectory relative to the webapp home.
     */
    private void renderHeadInternal(IHeaderResponse response) {
        response.render(JavaScriptHeaderItem.forReference(getApplication()
                        .getJavaScriptLibrarySettings()
                        .getJQueryReference()));
        String modules = getStringResource(JS_MODULES);
        if (!MISSING_RESOURCE_KEY.equals(modules)) {
            String[] jsrefs = modules.split("[,]");
            for (String js : jsrefs) {
                response.render(JavaScriptHeaderItem.forUrl("js/" + js + ".js"));
            }
        }
    }

    private void setTimeout() {
        ServletWebRequest webRequest = (ServletWebRequest) getRequest();

        if (getWebadminSession().isSignedIn()) {
            webRequest.getContainerRequest()
                      .getSession()
                      .setMaxInactiveInterval((int)TimeUnit.MINUTES.toSeconds(30));
        } else {
            webRequest.getContainerRequest()
                      .getSession()
                      .setMaxInactiveInterval((int)TimeUnit.DAYS.toSeconds(1));
        }
    }
}
