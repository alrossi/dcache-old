/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2023 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.webdav;

import io.milton.http.exceptions.BadRequestException;
import io.milton.http.exceptions.NotAuthorizedException;
import io.milton.http.values.ValueAndType;
import io.milton.http.webdav.PropFindPropertyBuilder;
import io.milton.http.webdav.PropFindResponse;
import io.milton.http.webdav.PropertiesRequest;
import io.milton.resource.PropFindableResource;
import io.milton.resource.Resource;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Set;
import javax.xml.namespace.QName;

import static java.util.Objects.requireNonNull;

/**
 * An implementation of PropFindPropertyBuilder that forwards to some wrapped instance.
 */
public class ForwardingPropFindPropertyBuilder implements PropFindPropertyBuilder {

    private final PropFindPropertyBuilder inner;

    public ForwardingPropFindPropertyBuilder(PropFindPropertyBuilder inner) {
        this.inner = requireNonNull(inner);
    }

    @Override
    public List<PropFindResponse> buildProperties(PropFindableResource pfr, int depth,
            PropertiesRequest parseResult, String url) throws URISyntaxException,
            NotAuthorizedException, BadRequestException {
        return inner.buildProperties(pfr, depth, parseResult, url);
    }

    @Override
    public ValueAndType getProperty(QName field, Resource resource) throws NotAuthorizedException,
              BadRequestException {
        return inner.getProperty(field, resource);
    }

    @Override
    public void processResource(List<PropFindResponse> responses, PropFindableResource resource,
            PropertiesRequest parseResult, String href, int requestedDepth, int currentDepth,
            String collectionHref) throws NotAuthorizedException, BadRequestException {
        inner.processResource(responses, resource, parseResult, href, requestedDepth, currentDepth,
                collectionHref);
    }

    @Override
    public Set<QName> findAllProps(PropFindableResource resource) throws NotAuthorizedException,
            BadRequestException {
        return inner.findAllProps(resource);
    }
}
