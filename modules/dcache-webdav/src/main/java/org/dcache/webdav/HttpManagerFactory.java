package org.dcache.webdav;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableMap;
import com.google.common.net.MediaType;
import io.milton.config.HttpManagerBuilder;
import io.milton.http.AbstractWrappingResponseHandler;
import io.milton.http.Auth;
import io.milton.http.AuthenticationService;
import io.milton.http.HandlerHelper;
import io.milton.http.HttpManager;
import io.milton.http.Response;
import io.milton.http.Response.Status;
import io.milton.http.exceptions.BadRequestException;
import io.milton.http.exceptions.NotAuthorizedException;
import io.milton.http.http11.DefaultHttp11ResponseHandler;
import io.milton.http.webdav.DefaultPropFindRequestFieldParser;
import io.milton.http.webdav.DefaultWebDavResponseHandler;
import io.milton.http.webdav.MsPropFindRequestFieldParser;
import io.milton.http.webdav.PropFindPropertyBuilder;
import io.milton.http.webdav.PropFindResponse;
import io.milton.http.webdav.PropFindXmlGenerator;
import io.milton.http.webdav.PropertiesRequest;
import io.milton.resource.PropFindableResource;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import org.dcache.http.PathMapper;
import org.dcache.webdav.federation.FederationResponseHandler;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Required;

public class HttpManagerFactory extends HttpManagerBuilder implements FactoryBean {

    private static ThreadLocal<PropertiesRequest> PROPERTY_REQUEST = new ThreadLocal<>();

    enum DefaultProperties {
        PERFORMANCE,
        MICROSOFT_COMPATIBLE
    };

    private ReloadableTemplate _template;
    private ImmutableMap<String, String> _templateConfig;
    private String _staticContentPath;
    private PathMapper _pathMapper;
    private DefaultProperties _defaultProperties = DefaultProperties.MICROSOFT_COMPATIBLE;

    public static Optional<PropertiesRequest> propertiesRequest() {
        return Optional.ofNullable(PROPERTY_REQUEST.get());
    }

    @Override
    public Object getObject() throws Exception {
        DcacheHtmlResponseHandler htmlResponseHandler = new DcacheHtmlResponseHandler();

        DcacheSimpleResponseHandler simpleResponseHandler = new DcacheSimpleResponseHandler();

        AcceptAwareResponseHandler acceptAware = new AcceptAwareResponseHandler();
        acceptAware.addResponse(MediaType.HTML_UTF_8, htmlResponseHandler);
        acceptAware.addResponse(MediaType.PLAIN_TEXT_UTF_8, simpleResponseHandler);
        acceptAware.setDefaultResponse(MediaType.PLAIN_TEXT_UTF_8);

        WorkaroundsResponseHandler workarounds = WorkaroundsResponseHandler.wrap(acceptAware);
        workarounds.setPathMapper(_pathMapper);

        Rfc3230ResponseHandler rfc3230 = Rfc3230ResponseHandler.wrap(workarounds);
        AbstractWrappingResponseHandler handler = new FederationResponseHandler(rfc3230);
        setWebdavResponseHandler(handler);

        var defaultFieldParser = new DefaultPropFindRequestFieldParser();
        var fieldParser = _defaultProperties == DefaultProperties.PERFORMANCE
            ? new DcachePropFindRequestFieldParser(defaultFieldParser)
            : new MsPropFindRequestFieldParser(defaultFieldParser);
        setPropFindRequestFieldParser(fieldParser);

        init();

        // Late initialization of handlers because AuthenticationService and
        // other collaborators have to be created first.

        DefaultWebDavResponseHandler miltonDefaultHandler =
              new DefaultWebDavResponseHandler(getHttp11ResponseHandler(),
                    getResourceTypeHelper(), getPropFindXmlGenerator());

        workarounds.setAuthenticationService(getAuthenticationService());

        htmlResponseHandler.setWrapped(miltonDefaultHandler);
        htmlResponseHandler.setReloadableTemplate(_template);
        htmlResponseHandler.setTemplateConfig(_templateConfig);
        htmlResponseHandler.setStaticContentPath(_staticContentPath);

        simpleResponseHandler.setWrapped(miltonDefaultHandler);

        handler.setBuffering(getBuffering());

        return buildHttpManager();
    }

    @Override
    protected PropFindPropertyBuilder propFindPropertyBuilder() {
        if (super.getPropFindPropertyBuilder() == null) {
            var inner = super.propFindPropertyBuilder();
            var newBuilder = new ForwardingPropFindPropertyBuilder(inner) {
                @Override
                public List<PropFindResponse> buildProperties(PropFindableResource pfr, int depth,
                            PropertiesRequest parseResult, String url) throws URISyntaxException,
                            NotAuthorizedException, BadRequestException {
                    PROPERTY_REQUEST.set(parseResult);
                    try {
                        return super.buildProperties(pfr, depth, parseResult, url);
                    } finally {
                        PROPERTY_REQUEST.remove();
                    }
                }
            };
            super.setPropFindPropertyBuilder(newBuilder);
        }
        return super.getPropFindPropertyBuilder();
    }


    /* The following hack allows injection of custom objects part way through init */
    @Override
    protected void buildResourceTypeHelper() {
        super.buildResourceTypeHelper();

        if (handlerHelper == null) {
            handlerHelper = new HandlerHelper(authenticationService);
            showLog("handlerHelper", handlerHelper);
        }

        if (propFindXmlGenerator == null) {
            propFindXmlGenerator = new PropFindXmlGenerator(valueWriters);
            showLog("propFindXmlGenerator", propFindXmlGenerator);
        }

        if (http11ResponseHandler == null) {
            DefaultHttp11ResponseHandler rh = createDefaultHttp11ResponseHandler(
                  authenticationService);
            rh.setCacheControlHelper(cacheControlHelper);
            rh.setBuffering(buffering);
            http11ResponseHandler = rh;
            showLog("http11ResponseHandler", http11ResponseHandler);
        }

        if (webdavResponseHandler == null) {
            webdavResponseHandler = new DefaultWebDavResponseHandler(http11ResponseHandler,
                  resourceTypeHelper, propFindXmlGenerator);
        }
        outerWebdavResponseHandler = webdavResponseHandler;

        if (resourceHandlerHelper == null) {
            resourceHandlerHelper = new DcacheResourceHandlerHelper(handlerHelper,
                  urlAdapter, outerWebdavResponseHandler, authenticationService);
            showLog("resourceHandlerHelper", resourceHandlerHelper);
        }
    }

    @Override
    protected DefaultHttp11ResponseHandler createDefaultHttp11ResponseHandler(
          AuthenticationService authenticationService) {
        // Subclass DefaultHttp11ResponseHandler to avoid adding a "Server" response header.
        return new DefaultHttp11ResponseHandler(authenticationService,
              eTagGenerator, contentGenerator) {
            @Override
            protected void setRespondCommonHeaders(Response response,
                  io.milton.resource.Resource resource, Status status, Auth auth) {
                response.setStatus(status);
                // The next line is omitted to avoid setting the Server header
                // response.setNonStandardHeader("Server", "milton.io-" + miltonVerson);
                response.setDateHeader(new Date());
                response.setNonStandardHeader("Accept-Ranges", "bytes");
                String etag = eTagGenerator.generateEtag(resource);
                if (etag != null) {
                    response.setEtag(etag);
                }
            }
        };
    }

    @Override
    public Class<?> getObjectType() {
        return HttpManager.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    @Required
    public void setPathMapper(PathMapper mapper) {
        _pathMapper = requireNonNull(mapper);
    }

    /**
     * Sets the resource containing the StringTemplateGroup for directory listing.
     */
    @Required
    public void setTemplate(ReloadableTemplate template) {
        _template = template;
    }

    @Required
    public void setTemplateConfig(ImmutableMap<String, String> config) {
        _templateConfig = config;
    }

    @Required
    public void setDefaultProperties(DefaultProperties properties) {
        _defaultProperties = requireNonNull(properties);
    }

    /**
     * The static content path is the path under which the service exports the static content. This
     * typically contains stylesheets and image files.
     */
    public void setStaticContentPath(String path) {
        _staticContentPath = path;
    }
}
