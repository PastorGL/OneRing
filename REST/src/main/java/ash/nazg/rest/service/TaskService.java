/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.rest.service;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.StringWriter;
import java.util.Properties;

@Singleton
public class TaskService {
    private RunService runService;

    @Inject
    public TaskService(RunService runService) {
        this.runService = runService;
    }

    public String validateTask(TaskDefinitionLanguage.Task task) throws Exception {
        StringWriter sw = new StringWriter();
        PropertiesWriter.writeProperties(task, sw);
        return sw.toString();
    }

    public TaskDefinitionLanguage.Task validateTask(Properties variables, Properties props) {
        return toTask(variables, props);
    }

    public String runOnTC(TaskDefinitionLanguage.Task task) throws Exception {
        return runService.defineTC(task);
    }

    public String runOnTC(Properties variables, Properties props) throws Exception {
        return runOnTC(toTask(variables, props));
    }

    public String localRun(TaskDefinitionLanguage.Task task) throws Exception {
        return runService.defineLocal(task);
    }

    public String localRun(Properties variables, Properties props) throws Exception {
        return localRun(toTask(variables, props));
    }

    public TaskStatus status(String taskId) throws Exception {
        return runService.status(taskId);
    }

    private TaskDefinitionLanguage.Task toTask(Properties variables, Properties properties) {
        return PropertiesReader.toTask(properties, variables);
    }
}
