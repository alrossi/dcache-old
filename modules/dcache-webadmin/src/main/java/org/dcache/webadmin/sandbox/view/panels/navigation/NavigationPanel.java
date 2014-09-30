package org.dcache.webadmin.sandbox.view.panels.navigation;

import org.apache.wicket.AttributeModifier;
import org.apache.wicket.authroles.authorization.strategies.role.Roles;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.link.BookmarkablePageLink;
import org.apache.wicket.markup.html.list.ListItem;
import org.apache.wicket.markup.html.list.ListView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import org.dcache.webadmin.sandbox.controller.services.login.Role;
import org.dcache.webadmin.sandbox.view.BaseWebadminPage;
import org.dcache.webadmin.sandbox.view.panels.BaseWebadminPanel;

/**
 * Reusable navigation-panel.
 */
public class NavigationPanel extends BaseWebadminPanel {
    private class LinkListView<T> extends ListView<T> {

        private static final long serialVersionUID = 4665791178375173441L;

        public LinkListView(String id, List<? extends T> items) {
            super(id, items);
        }

        @Override
        protected void populateItem(ListItem item) {
            Class targetPage = (Class) item.getModelObject();
            BookmarkablePageLink link = new BookmarkablePageLink("link",
                                                                 targetPage);
            handleAdminPage(targetPage, item);
            setLinkTitle(link, item.getIndex());
            handleActivePage(targetPage, item);
            item.add(link);
        }

        private void addActiveAttribute(ListItem item) {
            item.add(new AttributeModifier("class", "active"));
        }

        private void addAdminOnlyTooltip(ListItem item) {
            item.add(new AttributeModifier("title",
                                            getStringResource("tooltip.AdminOnly")
                                            .getObject()));
        }

        private void handleActivePage(Class targetPage, ListItem item) {
            if (isActivePage(targetPage)) {
                addActiveAttribute(item);
            }
        }

        private void handleAdminPage(Class targetPage, ListItem item) {
            if (isAdminPage(targetPage)) {
                if (!isUserAdmin()) {
                    addAdminOnlyTooltip(item);
                }
            }
        }

        private boolean isActivePage(Class targetPage) {
            return targetPage.equals(currentPage);
        }

        private boolean isAdminPage(Class targetPage) {
            return ((BaseWebadminPage) getPage()).getWebadminApplication()
                                                 .isAdminPage(targetPage);
        }

        private boolean isUserAdmin() {
            return ((BaseWebadminPage) getPage()).getWebadminSession().
                    hasAnyRole(new Roles(Role.ADMIN.toString()));
        }

        private void setLinkTitle(BookmarkablePageLink link, int linkNumber) {
            link.add(new Label("linkTitle", LINK_NAMES.get(linkNumber)));
        }
    }

    private static final long serialVersionUID = 4803403315602047391L;
    private static final Logger LOGGER = LoggerFactory.getLogger(NavigationPanel.class);
    private static final List<String> LINK_NAMES = new ArrayList<>();
    private static final List<Class<? extends BaseWebadminPage>> LINKS = new ArrayList<>();

    Class<?> currentPage;

    public static void addLink(String name,
                               Class<? extends BaseWebadminPage> page) {
        LINK_NAMES.add(name);
        LINKS.add(page);
    }

    public NavigationPanel(String id, Class currentPage) {
        super(id);
        this.currentPage = currentPage;
        LOGGER.debug(currentPage.getSimpleName());
        add(new LinkListView<Class>("linkList", LINKS));
    }
}
