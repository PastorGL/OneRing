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
import java.util.ArrayList;
import java.util.List;
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
    public Response validateTasks(@Valid List<TaskDefinitionLanguage.Task> tasks) {
        try {
            List<String> ini = new ArrayList<>();
            for (TaskDefinitionLanguage.Task task : tasks) {
                ini.addAll(taskService.validateTask(task).entrySet().stream()
                        .map(e -> e.getKey() + "=" + e.getValue())
                        .collect(Collectors.toList()));
            }

            return Response.ok(String.join("\n", ini)).build();
        } catch (Exception mess) {
            return Response.status(Response.Status.BAD_REQUEST).entity(mess.getMessage()).build();
        }
    }

    @POST
    @Path("validate.ini")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.APPLICATION_JSON)
    public Response validateTasks(@QueryParam("prefix") String prefixes, @NotEmpty String properties) throws IOException {
        Properties props = new Properties();
        props.load(new StringReader(properties));

        try {
            return Response.ok(taskService.validateTasks(prefixes, props)).build();
        } catch (Exception mess) {
            return Response.status(Response.Status.BAD_REQUEST).entity(mess.getMessage()).build();
        }
    }

    @POST
    @Path("run/local.json")
    @Consumes(MediaType.APPLICATION_JSON)
    public List<String> localRunTask(@QueryParam("variables") String variables, @Valid List<TaskDefinitionLanguage.Task> tasks) {
        return taskService.localRun(variables, tasks);
    }

    @POST
    @Path("run/local.ini")
    @Consumes(MediaType.TEXT_PLAIN)
    public List<String> localRunTask(@QueryParam("prefix") String prefixes, @QueryParam("variables") String variables, @NotEmpty String properties) throws Exception {
        Properties props = new Properties();
        props.load(new StringReader(properties));

        return taskService.localRun(prefixes, variables, props);
    }

    @POST
    @Path("run/remote/tc.json")
    @Consumes(MediaType.APPLICATION_JSON)
    public List<String> runTaskOnTC(@QueryParam("variables") String variables, @Valid List<TaskDefinitionLanguage.Task> tasks) throws Exception {
        return taskService.runOnTC(variables, tasks);
    }

    @POST
    @Path("run/remote/tc.ini")
    @Consumes(MediaType.TEXT_PLAIN)
    public List<String> runTaskOnTC(@QueryParam("prefix") String prefixes, @QueryParam("variables") String variables, @NotEmpty String properties) throws Exception {
        Properties props = new Properties();
        props.load(new StringReader(properties));

        return taskService.runOnTC(prefixes, variables, props);
    }

    @GET
    @Path("status")
    @Produces(MediaType.TEXT_PLAIN)
    public String status(@QueryParam("taskId") String taskId) throws Exception {
        return taskService.status(taskId).name();
    }
}
