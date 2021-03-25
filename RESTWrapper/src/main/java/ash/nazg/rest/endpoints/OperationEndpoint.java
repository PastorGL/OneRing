package ash.nazg.rest.endpoints;

import ash.nazg.config.tdl.TaskDocumentationLanguage;
import ash.nazg.rest.service.PackageService;

import javax.inject.Inject;
import javax.validation.constraints.NotEmpty;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("operation")
public class OperationEndpoint {
    private PackageService packageService;

    @Inject
    public OperationEndpoint(PackageService packageService) {
        this.packageService = packageService;
    }


    @GET
    @Path("{name}")
    @Produces(MediaType.APPLICATION_JSON)
    public TaskDocumentationLanguage.Operation packageOperations(@PathParam("name") @NotEmpty String name) throws Exception {
        return packageService.getOperation(name);
    }
}
