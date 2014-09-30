package org.dcache.webadmin.sandbox.view.pages.login;

import org.apache.wicket.Page;
import org.apache.wicket.authentication.IAuthenticationStrategy;
import org.apache.wicket.markup.html.WebMarkupContainer;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.form.Button;
import org.apache.wicket.markup.html.form.CheckBox;
import org.apache.wicket.markup.html.form.PasswordTextField;
import org.apache.wicket.markup.html.form.StatelessForm;
import org.apache.wicket.markup.html.form.TextField;
import org.apache.wicket.markup.html.panel.FeedbackPanel;
import org.apache.wicket.model.CompoundPropertyModel;
import org.apache.wicket.protocol.https.RequireHttps;
import org.apache.wicket.request.RequestHandlerStack.ReplaceHandlerException;
import org.apache.wicket.request.mapper.parameter.PageParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dcache.webadmin.sandbox.controller.beans.LogInBean;
import org.dcache.webadmin.sandbox.controller.beans.UserBean;
import org.dcache.webadmin.sandbox.controller.exceptions.LogInServiceException;
import org.dcache.webadmin.sandbox.view.BaseWebadminPage;
import org.dcache.webadmin.sandbox.view.panels.user.UserPanel;

/**
 * Not sure I want to make this extend BaseWebadminPage
 * @author arossi
 *
 */
@RequireHttps
public class LogIn extends BaseWebadminPage {

    class LogInButton extends Button {
        private static final long serialVersionUID = -8852712258475979167L;

        public LogInButton(String id) {
            super(id);
        }

        @Override
        public void onSubmit() {
            IAuthenticationStrategy strategy = getApplication()
                                               .getSecuritySettings()
                                               .getAuthenticationStrategy();
            try {
                if (!getWebadminSession().isSignedIn()) {
                    signIn(logInModel, strategy);
                }
                setGoOnPage();
            } catch (LogInServiceException ex) {
                strategy.remove();
                String cause = "unknown";
                if (ex.getMessage() != null) {
                    cause = ex.getMessage();
                }
                error(getStringResource("loginError") + " - cause: " + cause);
                LOGGER.debug("user/pwd sign in error - cause {}", cause);
            }
        }
    }

    class LogInForm extends StatelessForm {
        private static final long serialVersionUID = -1800491058587279179L;

        LogInForm(final String id) {
            super(id, new CompoundPropertyModel<>(new LogInBean()));
            logInModel = (LogInBean) getDefaultModelObject();

            user = new TextField("username");
            user.setRequired(true);
            add(user);

            password = new PasswordTextField("password");
            add(password);

            add(new LogInButton("submit"));

            rememberMeRow = new WebMarkupContainer("rememberMeRow");
            add(rememberMeRow);

            rememberMe = new CheckBox("remembering");
            rememberMeRow.add(rememberMe);
        }

    }

    /**
     * There seems to be an unresolved issue in the way the base url is
     * determined for the originating page. It seems that if the
     * RestartResponseAtInterceptPageException is thrown from within a
     * subcomponent of the page, the url reflects in its query part that
     * subcomponent, and consequently on redirect after intercept, Wicket
     * attempts to return to it, generating an Access Denied Page.
     * <p>
     * At present I see no other way of maintaining proper intercept- redirect
     * behavior except via a workaround which defeats the exception handling
     * mechanism by setting the response page, determined via the constructor.
     * Panels using the intercept exception (for example, {@link UserPanel})
     * should set the first page parameter to be the originating page class
     * name.
     * <p>
     * This behavior has been present since Wicket 1.5. See <a
     * href="http://apache-wicket.1842946.n4.nabble.com/\
     * RestartResponseAtInterceptPageException-problem-in-1-5-td4255020.html">\
     * RestartAtIntercept problem</a>.
     */
    private void setGoOnPage() {
        /*
         * If login has been called because the user was not yet logged in, then
         * continue to the original destination, otherwise to the Home page.
         */
        try {
            continueToOriginalDestination();
        } catch (ReplaceHandlerException e) {
            /*
             * Note that #continueToOriginalDestination should use this
             * exception to return to the calling page, with no exception thrown
             * if this page was not invoked as an intercept page. Hence catching
             * this exception is technically incorrect behavior, according to
             * the Wicket specification. But see the documentation to this
             * method.
             */
        }

        setResponsePage(returnPage);
    }

    private void signIn(LogInBean model, IAuthenticationStrategy strategy)
                    throws LogInServiceException {
        String username;
        String password;
        if (model != null) {
            username = model.getUsername();
            password = model.getPassword();
        } else {
            /*
             * get username and password from persistent store
             */
            String[] data = strategy.load();
            if ((data == null) || (data.length <= 1)) {
                throw new LogInServiceException("no username data saved");
            }
            username = data[0];
            password = data[1];
        }
        LOGGER.info("username sign in, username: {}", username);
        UserBean user = getWebadminApplication()
                        .getLoginService()
                        .authenticate(username, password.toCharArray());
        getWebadminSession().setUser(user);
        if (model != null && model.isRemembering()) {
            strategy.save(username, password);
        } else {
            strategy.remove();
        }
    }

    private static final long serialVersionUID = 8902191632839916396L;
    private static final Logger LOGGER = LoggerFactory.getLogger(LogIn.class);

    private Class<? extends Page> returnPage;
    private TextField user;
    private PasswordTextField password;
    private CheckBox rememberMe;
    private WebMarkupContainer rememberMeRow;
    private LogInBean logInModel;

    public LogIn() {
        super();
        this.returnPage = getApplication().getHomePage();
    }

    public LogIn(PageParameters pageParameters) throws ClassNotFoundException {
        super(pageParameters);
        this.returnPage = (Class<? extends Page>)BaseWebadminPage.class
                            .getClassLoader()
                            .loadClass(pageParameters.get(0).toString());
    }

    protected void initialize() {
        super.initialize();
        final FeedbackPanel feedback = new FeedbackPanel("feedback");
        add(new Label("dCacheInstanceName",
                        getWebadminApplication().getDcacheName()));
        add(feedback);
        add(new LogInForm("LogInForm"));
    }

    @Override
    protected void initializeProvider() {
    }
}
