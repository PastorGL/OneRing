package ash.nazg.rest.endpoints;

import javax.ws.rs.GET;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.Path;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.EntityTag;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;

@Path("docs")
public class StaticDocumentationEndpoint {
    @GET
    @Path("{all:.+}")
    public Response handle() {
        return Response.status(Response.Status.NOT_FOUND).build();
    }

    @Provider
    public static class StaticDocumentationProvider implements ContainerRequestFilter {
        @Override
        public void filter(ContainerRequestContext requestContext) throws IOException {
            UriInfo uriInfo = requestContext.getUriInfo();

            String path = uriInfo.getPath();

            String method = requestContext.getMethod();
            if (path.startsWith("docs/") && (path.endsWith(".md") || path.endsWith("example.json") || path.endsWith("example.ini"))
                    && (method.equals(HttpMethod.GET) || method.equals(HttpMethod.HEAD))) {
                URL res = getClass().getClassLoader().getResource(path);

                if (res != null) {
                    URLConnection conn = res.openConnection();

                    EntityTag etag = new EntityTag(Long.toHexString(((conn.getLastModified() / 1000L) << 20) + conn.getContentLength()));

                    Response resp;
                    Response.ResponseBuilder rb = requestContext.getRequest().evaluatePreconditions(etag);
                    if (rb != null) {
                        resp = rb.build();
                    } else {
                        resp = Response.ok(conn.getContent()).tag(etag).build();
                    }

                    requestContext.abortWith(resp);
                }
            }
        }
    }
}
