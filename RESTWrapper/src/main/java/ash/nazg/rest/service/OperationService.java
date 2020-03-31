/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.rest.service;

import ash.nazg.config.tdl.DocumentationGenerator;
import ash.nazg.spark.OpInfo;
import ash.nazg.spark.Operation;
import ash.nazg.spark.Operations;
import ash.nazg.config.tdl.TaskDefinitionLanguage;
import ash.nazg.config.tdl.TaskDescriptionLanguage;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.StringWriter;
import java.io.Writer;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

@Singleton
public class OperationService {
    private final TaskService taskService;

    @Inject
    public OperationService(TaskService taskService) {
        this.taskService = taskService;
    }

    public List<TaskDescriptionLanguage.Operation> getAvailableOperations() {
        return Operations.getAvailableOperations().values().stream()
                .map(OpInfo::description)
                .collect(Collectors.toList());
    }

    public TaskDescriptionLanguage.Operation getOperation(String verb) {
        return Operations.getAvailableOperations().get(verb).description();
    }

    public TaskDefinitionLanguage.Task example(String verb) {
        return createExample(null, verb);
    }

    public Properties example(String prefix, String verb) {
        return taskService.validateTask(createExample(prefix, verb));
    }

    private TaskDefinitionLanguage.Task createExample(String prefix, String verb) {
        OpInfo opInfo = Operations.getAvailableOperations().get(verb);

        if (opInfo == null) {
            return null;
        }

        return DocumentationGenerator.createExampleTask(opInfo, prefix);
    }

    public String doc(String verb) {
        try (Writer writer = new StringWriter()) {
            DocumentationGenerator.operationDoc(Operations.getAvailableOperations().get(verb), writer);

            return writer.toString();
        } catch (Exception ignore) {
        }

        return null;
    }
}
