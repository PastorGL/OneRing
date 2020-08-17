/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.rest.endpoints;

import ash.nazg.config.tdl.DocumentationGenerator;
import ash.nazg.config.tdl.TaskDefinitionLanguage;
import ash.nazg.config.tdl.TaskDescriptionLanguage;
import ash.nazg.rest.service.OperationService;
import org.hibernate.validator.constraints.NotEmpty;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.io.StringWriter;
import java.util.List;

@Path("operation")
public class OperationEndpoint {
    private OperationService operationService;

    @Inject
    public OperationEndpoint(OperationService operationService) {
        this.operationService = operationService;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<TaskDescriptionLanguage.Operation> getAvailableOperations() {
        return operationService.getAvailableOperations();
    }

    @GET
    @Path("{verb}")
    @Produces(MediaType.APPLICATION_JSON)
    public TaskDescriptionLanguage.Operation getOperationDescription(@PathParam("verb") @NotEmpty String verb) {
        return operationService.getOperation(verb);
    }

    @GET
    @Path("{verb}/example.ini")
    @Produces(MediaType.TEXT_PLAIN)
    public String taskExample(@QueryParam("prefix") String prefix, @PathParam("verb") @NotEmpty String verb) throws Exception {
        StringWriter sw = new StringWriter();

        DocumentationGenerator.writeDoc(operationService.example(prefix, verb), sw);
        return sw.toString();
    }

    @GET
    @Path("{verb}/example.json")
    @Produces(MediaType.APPLICATION_JSON)
    public TaskDefinitionLanguage.Task taskExampleJson(@PathParam("verb") @NotEmpty String verb) {
        return operationService.example(verb);
    }

    @GET
    @Path("{verb}.md")
    @Produces("text/markdown")
    public String operationDoc(@PathParam("verb") @NotEmpty String verb) {
        return operationService.doc(verb);
    }
}
