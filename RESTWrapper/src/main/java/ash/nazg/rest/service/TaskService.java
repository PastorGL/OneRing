/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.rest.service;

import ash.nazg.config.WrapperConfig;
import ash.nazg.config.tdl.PropertiesConverter;
import ash.nazg.config.tdl.TaskDefinitionLanguage;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Properties;

@Singleton
public class TaskService {
    private RunService runService;

    @Inject
    public TaskService(RunService runService) {
        this.runService = runService;
    }

    public Properties validateTask(TaskDefinitionLanguage.Task task) {
        return PropertiesConverter.toProperties(task);
    }

    public TaskDefinitionLanguage.Task validateTask(String prefix, Properties props) throws Exception {
        return toTask(prefix, props);
    }

    public String runOnTC(String variables, TaskDefinitionLanguage.Task task) throws Exception {
        return runService.defineTC(task, variables);
    }

    public String runOnTC(String prefix, String variables, Properties props) throws Exception {
        return runOnTC(variables, toTask(prefix, props));
    }

    public String localRun(String variables, TaskDefinitionLanguage.Task task) {
        return runService.defineLocal(task, variables);
    }

    public String localRun(String prefix, String variables, Properties props) throws Exception {
        return localRun(variables, toTask(prefix, props));
    }

    public TaskStatus status(String taskId) throws Exception {
        return runService.status(taskId);
    }

    private TaskDefinitionLanguage.Task toTask(String prefix, Properties properties) throws Exception {
        WrapperConfig taskConfig = new WrapperConfig();
        taskConfig.setPrefix(prefix);
        taskConfig.setProperties(properties);

        return PropertiesConverter.toTask(taskConfig);
    }
}
