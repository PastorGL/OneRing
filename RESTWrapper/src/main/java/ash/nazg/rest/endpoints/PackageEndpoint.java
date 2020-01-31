/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.rest.endpoints;

import ash.nazg.rest.service.PackageService;
import org.hibernate.validator.constraints.NotEmpty;

import javax.inject.Inject;
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
    @Path("{name}")
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> packageOperations(@PathParam("name") @NotEmpty String name) {
        return packageService.getPackage(name);
    }

    @GET
    @Path("{name}.md")
    @Produces("text/markdown")
    public String packageDoc(@PathParam("name") @NotEmpty String name) {
        return packageService.getPackageDoc(name);
    }
}
