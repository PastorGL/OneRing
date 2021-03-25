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

@Path("adapter")
public class AdapterEndpoint {
    private PackageService packageService;

    @Inject
    public AdapterEndpoint(PackageService packageService) {
        this.packageService = packageService;
    }

    @GET
    @Path("input/{name}")
    @Produces(MediaType.APPLICATION_JSON)
    public TaskDocumentationLanguage.Adapter packageInputAdapters(@PathParam("name") @NotEmpty String name) throws Exception {
        return packageService.getInputAdapter(name);
    }

    @GET
    @Path("output/{name}")
    @Produces(MediaType.APPLICATION_JSON)
    public TaskDocumentationLanguage.Adapter packageOutputAdapters(@PathParam("name") @NotEmpty String name) throws Exception {
        return packageService.getOutputAdapter(name);
    }

}
