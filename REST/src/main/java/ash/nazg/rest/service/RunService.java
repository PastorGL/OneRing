/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.rest.service;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class RunService {
    private LocalRunnerService localRunner;
    private TCRunnerService tcRunner;

    @Inject
    public RunService(LocalRunnerService localRunner, TCRunnerService tcRunner) {
        this.localRunner = localRunner;
        this.tcRunner = tcRunner;
    }

    public String defineLocal(TaskDefinitionLanguage.Task task) throws Exception {
        return "local:" + localRunner.define(task);
    }

    public String defineTC(TaskDefinitionLanguage.Task task) throws Exception {
        return "tc:" + tcRunner.define(task);
    }

    public TaskStatus status(String taskId) throws Exception {
        String[] task = taskId.split(":", 2);

        return "local".equals(task[0])
                ? localRunner.status(task[1])
                : tcRunner.status(task[1]);
    }
}
