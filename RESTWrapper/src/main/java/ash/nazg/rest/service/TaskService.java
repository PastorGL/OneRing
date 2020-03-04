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
import java.util.ArrayList;
import java.util.List;
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

    public List<TaskDefinitionLanguage.Task> validateTasks(String prefixes, Properties props) throws Exception {
        return toTasks(prefixes, props);
    }

    public List<String> runOnTC(String variables, List<TaskDefinitionLanguage.Task> tasks) throws Exception {
        return runService.defineTC(tasks, variables);
    }

    public List<String> runOnTC(String prefixes, String variables, Properties props) throws Exception {
        return runOnTC(variables, toTasks(prefixes, props));
    }

    public List<String> localRun(String variables, List<TaskDefinitionLanguage.Task> tasks) {
        return runService.defineLocal(tasks, variables);
    }

    public List<String> localRun(String prefixes, String variables, Properties props) throws Exception {
        return localRun(variables, toTasks(prefixes, props));
    }

    public TaskStatus status(String taskId) throws Exception {
        return runService.status(taskId);
    }

    private List<TaskDefinitionLanguage.Task> toTasks(String prefixes, Properties properties) throws Exception {
        List<TaskDefinitionLanguage.Task> tasks = new ArrayList<>();

        for (String prefix : prefixes.split(",")) {
            WrapperConfig taskConfig = new WrapperConfig();
            taskConfig.setPrefix(prefix);
            taskConfig.setProperties(properties);

            tasks.add(PropertiesConverter.toTask(taskConfig));
        }

        return tasks;
    }
}
