package org.dcache.webadmin.sandbox.view.panels;

import org.apache.wicket.markup.html.panel.Panel;
import org.apache.wicket.model.IModel;
import org.apache.wicket.model.StringResourceModel;

public class BaseWebadminPanel extends Panel {

    private static final long serialVersionUID = -572941307837646077L;

    public BaseWebadminPanel(String id) {
        super(id);
    }

    protected IModel<String> getStringResource(String resourceKey) {
        return new StringResourceModel(resourceKey, this, null);
    }
}
