/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.rest.endpoints;

import ash.nazg.config.tdl.TaskDescriptionLanguage;
import ash.nazg.rest.service.PackageService;

import javax.inject.Inject;
import javax.validation.constraints.NotEmpty;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.List;

@Path("package")
public class PackageEndpoint {
    private PackageService packageService;

    @Inject
    public PackageEndpoint(PackageService packageService) {
        this.packageService = packageService;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> packageList() {
        return packageService.getPackages();
    }

    @GET
    @Path("{name}/listOperation")
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> packageOperationList(@PathParam("name") @NotEmpty String name) {
        return packageService.getOperationList(name);
    }

    @GET
    @Path("{name}/operation")
    @Produces(MediaType.APPLICATION_JSON)
    public List<TaskDescriptionLanguage.Operation> packageOperations(@PathParam("name") @NotEmpty String name) {
        return packageService.getOperations(name);
    }

    @GET
    @Path("{name}/listAdapter")
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> packageAdapters(@PathParam("name") @NotEmpty String name) {
        return packageService.getPackageAdapters(name);
    }
}
