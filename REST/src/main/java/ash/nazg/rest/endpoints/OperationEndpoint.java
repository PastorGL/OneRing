package ash.nazg.rest.endpoints;

import ash.nazg.config.tdl.metadata.OperationMeta;
import ash.nazg.scripting.Operations;

import javax.validation.constraints.NotEmpty;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("operation")
public class OperationEndpoint {
    @GET
    @Path("{name}")
    @Produces(MediaType.APPLICATION_JSON)
    public OperationMeta operation(@PathParam("name") @NotEmpty String name) {
        return Operations.OPERATIONS.get(name).meta;
    }
}
