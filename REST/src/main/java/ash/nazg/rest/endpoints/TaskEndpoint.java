/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.rest.endpoints;

import ash.nazg.config.tdl.TaskDefinitionLanguage;
import ash.nazg.rest.service.TaskService;

import javax.inject.Inject;
import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.StringReader;
import java.util.Base64;
import java.util.Properties;

@Path("task")
public class TaskEndpoint {
    private final TaskService taskService;

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
            TaskDefinitionLanguage.fixup(task);
            String ini = taskService.validateTask(task);

            return Response.ok(ini).build();
        } catch (Exception mess) {
            return Response.status(Response.Status.BAD_REQUEST).entity(mess.getMessage()).build();
        }
    }

    @POST
    @Path("validate.ini")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.APPLICATION_JSON)
    public Response validateTask(@QueryParam("variables") String variables, @NotEmpty String properties) {
        try {
            return Response.ok(taskService.validateTask(readVariables(variables), readProperties(properties))).build();
        } catch (Exception mess) {
            return Response.status(Response.Status.BAD_REQUEST).entity(mess.getMessage()).build();
        }
    }

    @POST
    @Path("run/local.json")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response localRunTask(@Valid TaskDefinitionLanguage.Task task) {
        try {
            TaskDefinitionLanguage.fixup(task);
            return Response.ok(taskService.localRun(task)).build();
        } catch (Exception mess) {
            return Response.status(Response.Status.BAD_REQUEST).entity(mess.getMessage()).build();
        }
    }

    @POST
    @Path("run/local.ini")
    @Consumes(MediaType.TEXT_PLAIN)
    public Response localRunTask(@QueryParam("variables") String variables, @NotEmpty String properties) {
        try {
            return Response.ok(taskService.localRun(readVariables(variables), readProperties(properties))).build();
        } catch (Exception mess) {
            return Response.status(Response.Status.BAD_REQUEST).entity(mess.getMessage()).build();
        }
    }

    @POST
    @Path("run/remote/tc.json")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response runTaskOnTC(@Valid TaskDefinitionLanguage.Task task) {
        try {
            TaskDefinitionLanguage.fixup(task);
            return Response.ok(taskService.runOnTC(task)).build();
        } catch (Exception mess) {
            return Response.status(Response.Status.BAD_REQUEST).entity(mess.getMessage()).build();
        }
    }

    @POST
    @Path("run/remote/tc.ini")
    @Consumes(MediaType.TEXT_PLAIN)
    public Response runTaskOnTC(@QueryParam("variables") String variables, @NotEmpty String properties) {
        try {
            return Response.ok(taskService.runOnTC(readVariables(variables), readProperties(properties))).build();
        } catch (Exception mess) {
            return Response.status(Response.Status.BAD_REQUEST).entity(mess.getMessage()).build();
        }

    }

    @GET
    @Path("status")
    @Produces(MediaType.TEXT_PLAIN)
    public Response status(@QueryParam("taskId") String taskId) {
        try {
            return Response.ok(taskService.status(taskId).name()).build();
        } catch (Exception mess) {
            return Response.status(Response.Status.BAD_REQUEST).entity(mess.getMessage()).build();
        }
    }

    private Properties readProperties(String properties) throws IOException {
        Properties props = new Properties();
        props.load(new StringReader(properties));

        return props;
    }

    private Properties readVariables(String variables) throws IOException {
        Properties overrides = new Properties();

        if (variables != null) {
            variables = new String(Base64.getDecoder().decode(variables));
            overrides.load(new StringReader(variables));
        }

        return overrides;
    }
}
