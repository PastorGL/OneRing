package ash.nazg.rest.service;

import ash.nazg.config.tdl.DocumentationGenerator;
import ash.nazg.spark.Operation;
import ash.nazg.spark.SparkTask;
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
        return SparkTask.getAvailableOperations().values().stream()
                .map(oi -> oi.description)
                .collect(Collectors.toList());
    }

    public TaskDescriptionLanguage.Operation getOperation(String verb) {
        return SparkTask.getAvailableOperations().get(verb).description;
    }

    public TaskDefinitionLanguage.Task example(String verb) {
        return createExample(null, verb);
    }

    public Properties example(String prefix, String verb) {
        return taskService.validateTask(createExample(prefix, verb));
    }

    private TaskDefinitionLanguage.Task createExample(String prefix, String verb) {
        Operation.Info opInfo = SparkTask.getAvailableOperations().get(verb);

        if (opInfo == null) {
            return null;
        }

        return DocumentationGenerator.createExampleTask(opInfo, prefix);
    }

    public String doc(String verb) {
        try (Writer writer = new StringWriter()) {
            DocumentationGenerator.operationDoc(SparkTask.getAvailableOperations().get(verb), writer);

            return writer.toString();
        } catch (Exception ignore) {
        }

        return null;
    }
}
