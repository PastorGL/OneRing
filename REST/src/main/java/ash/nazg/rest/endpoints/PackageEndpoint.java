/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.rest.endpoints;

import ash.nazg.config.tdl.TaskDocumentationLanguage;
import ash.nazg.rest.service.PackageService;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
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
    public List<TaskDocumentationLanguage.Package> packageList() throws Exception {
        return packageService.getPackages();
    }
}
