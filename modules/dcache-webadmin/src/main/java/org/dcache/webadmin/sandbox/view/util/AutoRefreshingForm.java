package org.dcache.webadmin.sandbox.view.util;

import org.apache.wicket.Component;
import org.apache.wicket.ajax.AjaxSelfUpdatingTimerBehavior;
import org.apache.wicket.markup.html.form.Form;
import org.apache.wicket.model.IModel;
import org.apache.wicket.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import org.dcache.webadmin.sandbox.view.BaseWebadminPage;

/**
 * @author arossi
 *
 */
public class AutoRefreshingForm<T> extends Form<T> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER
        = LoggerFactory.getLogger(AutoRefreshingForm.class);

    public AutoRefreshingForm(String id,
                              BaseWebadminPage parent) {
        super(id);
        addAutoRefreshBehavior(parent);
    }

    public AutoRefreshingForm(String id,
                              IModel<T> model,
                              BaseWebadminPage parent) {
        super(id, model);
        addAutoRefreshBehavior(parent);
    }

    public AutoRefreshingForm(String id,
                              long refresh,
                              TimeUnit unit,
                              BaseWebadminPage parent) {
        super(id);
        addAutoRefreshBehavior(refresh, unit, parent);
    }

    public AutoRefreshingForm(String id,
                              IModel<T> model,
                              long refresh,
                              TimeUnit unit,
                              BaseWebadminPage parent) {
        super(id, model);
        addAutoRefreshBehavior(refresh, unit, parent);
    }

    private void addAutoRefreshBehavior(BaseWebadminPage parent) {
        addAutoRefreshBehavior(1L, TimeUnit.MINUTES, parent);
    }

    private void addAutoRefreshBehavior(final long refresh,
                                        final TimeUnit unit,
                                        final BaseWebadminPage parent) {
        add(new AjaxSelfUpdatingTimerBehavior(
                        Duration.valueOf(unit.toMillis(refresh))) {
            private static final long serialVersionUID = 541235165961670681L;

            @Override
            public void beforeRender(Component component) {
                parent.refresh();
            }

            @Override
            protected boolean shouldTrigger() {
                LOGGER.trace("checking to see if {} should be triggered", this);
                return super.shouldTrigger();
            }
        });
    }
}
