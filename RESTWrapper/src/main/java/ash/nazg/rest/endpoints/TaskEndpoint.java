/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.rest.endpoints;

import ash.nazg.rest.service.TaskService;
import ash.nazg.config.tdl.TaskDefinitionLanguage;
import org.hibernate.validator.constraints.NotEmpty;

import javax.inject.Inject;
import javax.validation.Valid;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.StringReader;
import java.util.Properties;
import java.util.stream.Collectors;

@Path("task")
public class TaskEndpoint {
    private TaskService taskService;

    @Inject
    public TaskEndpoint(TaskService taskService) {
        this.taskService = taskService;
    }

    @POST
    @Path("validate.json")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    public Response validateTask(@Valid TaskDefinitionLanguage.Task task) {
        try {
            return Response.ok(taskService.validateTask(task).entrySet().stream()
                    .map(e -> e.getKey() + "=" + e.getValue())
                    .collect(Collectors.joining("\n"))).build();
        } catch (Exception mess) {
            return Response.status(Response.Status.BAD_REQUEST).entity(mess.getMessage()).build();
        }
    }

    @POST
    @Path("validate.ini")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.APPLICATION_JSON)
    public Response validateTask(@QueryParam("prefix") String prefix, @QueryParam("variables") String variables, @NotEmpty String properties) throws IOException {
        Properties props = new Properties();
        props.load(new StringReader(properties));

        try {
            return Response.ok(taskService.validateTask(prefix, props)).build();
        } catch (Exception mess) {
            return Response.status(Response.Status.BAD_REQUEST).entity(mess.getMessage()).build();
        }
    }

    @POST
    @Path("run/local.json")
    @Consumes(MediaType.APPLICATION_JSON)
    public String localRunTask(@QueryParam("variables") String variables, @Valid TaskDefinitionLanguage.Task task) throws Exception {
        return taskService.localRun(variables, task);
    }

    @POST
    @Path("run/local.ini")
    @Consumes(MediaType.TEXT_PLAIN)
    public String localRunTask(@QueryParam("prefix") String prefix, @QueryParam("variables") String variables, @NotEmpty String properties) throws Exception {
        Properties props = new Properties();
        props.load(new StringReader(properties));

        return taskService.localRun(prefix, variables, props);
    }

    @POST
    @Path("run/remote/tc.json")
    @Consumes(MediaType.APPLICATION_JSON)
    public String runTaskOnTC(@QueryParam("variables") String variables, @Valid TaskDefinitionLanguage.Task task) throws Exception {
        return taskService.runOnTC(variables, task);
    }

    @POST
    @Path("run/remote/tc.ini")
    @Consumes(MediaType.TEXT_PLAIN)
    public String runTaskOnTC(@QueryParam("prefix") String prefix, @QueryParam("variables") String variables, @NotEmpty String properties) throws Exception {
        Properties props = new Properties();
        props.load(new StringReader(properties));

        return taskService.runOnTC(prefix, variables, props);
    }

    @GET
    @Path("status")
    @Produces(MediaType.TEXT_PLAIN)
    public String status(@QueryParam("taskId") String taskId) throws Exception {
        return taskService.status(taskId).name();
    }
}
