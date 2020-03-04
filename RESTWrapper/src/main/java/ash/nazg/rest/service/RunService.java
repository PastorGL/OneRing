/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.rest.service;

import ash.nazg.config.tdl.TaskDefinitionLanguage;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.List;

@Singleton
public class RunService {
    private LocalRunnerService localRunner;
    private TCRunnerService tcRunner;

    @Inject
    public RunService(LocalRunnerService localRunner, TCRunnerService tcRunner) {
        this.localRunner = localRunner;
        this.tcRunner = tcRunner;
    }

    public List<String> defineLocal(List<TaskDefinitionLanguage.Task> tasks, String params) {
        List<String> taskIds = new ArrayList<>();
        for (TaskDefinitionLanguage.Task task : tasks) {
            taskIds.add("local:" + localRunner.define(task, params));
        }

        return taskIds;
    }

    public List<String> defineTC(List<TaskDefinitionLanguage.Task> tasks, String params) throws Exception {
        List<String> taskIds = new ArrayList<>();
        for (TaskDefinitionLanguage.Task task : tasks) {
            taskIds.add("local:" + tcRunner.define(task, params));
        }

        return taskIds;
    }

    public TaskStatus status(String taskId) throws Exception {
        String[] task = taskId.split(":", 2);

        return "local".equals(task[0])
                ? localRunner.status(task[1])
                : tcRunner.status(task[1]);
    }
}
