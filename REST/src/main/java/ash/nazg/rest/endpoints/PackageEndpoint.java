/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.rest.endpoints;

import ash.nazg.scripting.Operations;
import ash.nazg.storage.Adapters;

import javax.validation.constraints.NotEmpty;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.Map;
import java.util.stream.Collectors;

@Path("package")
public class PackageEndpoint {
    @GET
    @Path("inputs")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, String> inputPackages() {
        return Adapters.INPUT_PACKAGES;
    }

    @GET
    @Path("operations")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, String> operationPackages() {
        return Operations.PACKAGES;
    }

    @GET
    @Path("outputs")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, String> outputPackages() {
        return Adapters.OUTPUT_PACKAGES;
    }

    @GET
    @Path("inputs/{name}")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, String> inputAdapterList(@PathParam("name") @NotEmpty String name) {
        return Adapters.packageInputs(name).entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().meta.descr));
    }

    @GET
    @Path("operations/{name}")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, String> operationList(@PathParam("name") @NotEmpty String name) {
        return Operations.packageOperations(name).entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().meta.descr));
    }

    @GET
    @Path("outputs/{name}")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, String> outputAdapterList(@PathParam("name") @NotEmpty String name) {
        return Adapters.packageOutputs(name).entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().meta.descr));
    }
}
