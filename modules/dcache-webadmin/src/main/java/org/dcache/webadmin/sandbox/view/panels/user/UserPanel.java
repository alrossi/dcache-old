package org.dcache.webadmin.sandbox.view.panels.user;

import org.apache.wicket.RestartResponseAtInterceptPageException;
import org.apache.wicket.Session;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.link.Link;
import org.apache.wicket.model.PropertyModel;
import org.apache.wicket.request.mapper.parameter.PageParameters;

import org.dcache.webadmin.sandbox.WebadminSession;
import org.dcache.webadmin.sandbox.view.BaseWebadminPage;
import org.dcache.webadmin.sandbox.view.pages.home.Home;
import org.dcache.webadmin.sandbox.view.pages.login.LogIn;
import org.dcache.webadmin.sandbox.view.panels.BaseWebadminPanel;

/**
 *
 * This Panel is for displaying the login information for a user.
 * It adds a login link that redirects a user to the login page.
 * When user is logged in it displays the username.
 * The logout link invalidates a session and redirects the user to the home page.
 *
 * @author tanja
 */
public class UserPanel extends BaseWebadminPanel {

    private static final long serialVersionUID = -4419358909048041100L;

    public UserPanel(String id, BaseWebadminPage basePage) {
        super(id);

        final PageParameters parameters = new PageParameters();
        parameters.set(0, basePage.getClass().getName());

        add(new Label("username", new PropertyModel(this, "session.userName")));
        add(new Link("logout") {

            private static final long serialVersionUID = -7805117496020130503L;

            @Override
            public void onClick() {
                if (((WebadminSession) Session.get()).isSignedIn()) {
                    getSession().invalidate();
                    setResponsePage(Home.class);
                }
            }

            @Override
            public boolean isVisible() {
                return ((WebadminSession) Session.get()).isSignedIn();
            }
        });

        add(new Link("login") {

            private static final long serialVersionUID = -1031589310010810063L;

            @Override
            public void onClick() {
                throw new RestartResponseAtInterceptPageException(LogIn.class,
                                                                  parameters);
            }

            @Override
            public boolean isVisible() {
                return !((WebadminSession) Session.get()).isSignedIn();
            }
        });

    }
}
