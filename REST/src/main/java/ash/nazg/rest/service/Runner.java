/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.rest.service;

import ash.nazg.config.tdl.TaskDefinitionLanguage;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

public abstract class Runner {
    public abstract TaskStatus status(String taskId) throws Exception;

    public abstract String define(TaskDefinitionLanguage.Task task) throws Exception;

    protected static void removeTempDir(File tempDir) throws IOException {
        Files.walk(tempDir.toPath()).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
    }

    protected static File makeTempDir() throws IOException {
        return Files.createTempDirectory("one-ring-rest").toFile();
    }
}
