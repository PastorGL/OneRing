/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.rest.service;

import ash.nazg.config.tdl.DocumentationGenerator;
import ash.nazg.config.tdl.TaskDefinitionLanguage;
import ash.nazg.config.tdl.TaskDescriptionLanguage;
import ash.nazg.config.tdl.TaskDocumentationLanguage;
import ash.nazg.spark.OpInfo;
import ash.nazg.spark.Operations;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.StringWriter;
import java.io.Writer;
import java.util.List;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;

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

    public TaskDefinitionLanguage.Task example(String prefix, String verb) {
        return createExample(prefix, verb);
    }

    private TaskDefinitionLanguage.Task createExample(String prefix, String verb) {
        OpInfo opInfo = Operations.getAvailableOperations().get(verb);

        if (opInfo == null) {
            return null;
        }

        try {
            TaskDocumentationLanguage.Operation opDoc = DocumentationGenerator.operationDoc(opInfo);

            return DocumentationGenerator.createExampleTask(opInfo, opDoc, prefix);
        } catch (Exception ignore) {
        }

        return null;
    }

    public String doc(String verb) {
        OpInfo opInfo = Operations.getAvailableOperations().get(verb);

        if (opInfo == null) {
            return null;
        }

        try (Writer writer = new StringWriter()) {
            TaskDocumentationLanguage.Operation opDoc = DocumentationGenerator.operationDoc(opInfo);

            VelocityContext vc = new VelocityContext();
            vc.put("op", opDoc);

            Template operation = Velocity.getTemplate("operation.vm", UTF_8.name());
            operation.merge(vc, writer);

            return writer.toString();
        } catch (Exception ignore) {
        }

        return null;
    }
}
