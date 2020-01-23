package ash.nazg.rest.service;

import ash.nazg.config.tdl.TaskDefinitionLanguage;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

public abstract class Runner {
    public abstract TaskStatus status(String taskId) throws Exception;

    public abstract String define(TaskDefinitionLanguage.Task task, String params) throws Exception;

    protected static void removeTempDir(File tempDir) throws IOException {
        Files.walk(tempDir.toPath()).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
    }

    protected static File makeTempDir() throws IOException {
        return Files.createTempDirectory("one-ring-rest").toFile();
    }

    protected static void executeAndWait(File tempDir, String errorMessage, String... command) throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder(command);
        pb.directory(tempDir);
        Process p = pb.start();
        if (p.waitFor() > 0) {
            throw new InterruptedException(errorMessage);
        }
    }
}
