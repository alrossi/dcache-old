package org.dcache.webadmin.sandbox.view.pages.home;

import org.apache.wicket.markup.html.basic.Label;

import org.dcache.webadmin.sandbox.view.BaseWebadminPage;

/**
 * Main overview of all dCache-Services.
 *
 * It might be nice to add something to this face page.
 */
public class Home extends BaseWebadminPage {
    private static final long serialVersionUID = 6130566348881616211L;

    public Home() {
        add(new Label("dCacheInstanceName", getWebadminApplication()
                                            .getDcacheName()));
    }

    @Override
    protected void initializeProvider() {
    }
}
