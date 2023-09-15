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

import io.milton.http.webdav.PropFindRequestFieldParser;
import io.milton.http.webdav.PropertiesRequest;
import io.milton.http.webdav.WebDavProtocol;
import java.io.InputStream;
import java.util.List;
import static java.util.Objects.requireNonNull;
import javax.xml.namespace.QName;

/**
 * An implementation of PropFindRequestFieldParser that wraps some other PropFindRequestFieldParser
 * and adds a default set of properties if the client failed to provide any.  This is similar to
 * io.milton.http.webdav.MsPropFindRequestFieldParser, but provides a more dCache-friendly default
 * set of properties.
 */
public class DcachePropFindRequestFieldParser implements PropFindRequestFieldParser {

    private static final List<QName> DEFAULT_PROPERTIES = List.of(
            webDavProperty("creationdate"),
            webDavProperty("getlastmodified"),
            webDavProperty("displayname"),
            webDavProperty("resourcetype"),
            webDavProperty("getcontentlength"),
            webDavProperty("getetag"));

    private final PropFindRequestFieldParser wrapped;

    public DcachePropFindRequestFieldParser(PropFindRequestFieldParser inner) {
        wrapped = requireNonNull(inner);
    }

    @Override
    public PropertiesRequest getRequestedFields(InputStream in) {
        PropertiesRequest result = wrapped.getRequestedFields(in);

        if (!result.isAllProp() && result.getNames().isEmpty()) {
            DEFAULT_PROPERTIES.stream().forEach(result::add);
        }

        return result;
    }

    private static QName webDavProperty(String name) {
        return new QName(WebDavProtocol.NS_DAV.getName(), name);
    }
}
