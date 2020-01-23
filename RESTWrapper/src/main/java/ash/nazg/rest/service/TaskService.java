package ash.nazg.rest.service;

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
        return PropertiesConverter.toTask(prefix, props);
    }

    public String runOnTC(String variables, TaskDefinitionLanguage.Task task) throws Exception {
        return runService.defineTC(task, variables);
    }

    public String runOnTC(String prefix, String variables, Properties props) throws Exception {
        return runOnTC(variables, PropertiesConverter.toTask(prefix, props));
    }

    public String localRun(String variables, TaskDefinitionLanguage.Task task) throws Exception {
        return runService.defineLocal(task, variables);
    }

    public String localRun(String prefix, String variables, Properties props) throws Exception {
        return localRun(variables, PropertiesConverter.toTask(prefix, props));
    }

    public TaskStatus status(String taskId) throws Exception {
        return runService.status(taskId);
    }
}
