/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.composer.config;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.WrapperConfigBuilder;
import ash.nazg.config.tdl.PropertiesReader;
import ash.nazg.config.tdl.TDLObjectMapper;
import ash.nazg.config.tdl.TaskDefinitionLanguage;
import org.apache.commons.cli.Options;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.Properties;

public class ComposeWrapperConfigBuilder extends WrapperConfigBuilder {
    public ComposeWrapperConfigBuilder() {
        options = new Options();

        addRequiredOption("C", "configs", true, "Config files path=alias pairs, separated by a comma");
        addOption("X", "tasks", true, "Task prefix(es), if needed. If each is different, specify all as a comma-separated list. If all are same, specify it only once");
        addOption("V", "variables", true, "name=value pairs of substitution variables for the Spark config, separated by a newline and encoded to Base64");
        addOption("v", "variablesFile", true, "Path to variables file, name=value pairs per each line");
        addRequiredOption("M", "mapping", true, "Data stream mapping file");
        addRequiredOption("o", "output", true, "Full path to the composed output config file (for JSON format use .json extension)");
        addOption("F", "full", false, "Perform full compose (use outputs only from the last config in the chain)");
    }

    public TaskDefinitionLanguage.Task build(String prefix, String source, Properties variables) throws Exception {
        Properties ini = new Properties();

        Reader sourceReader = new BufferedReader(new FileReader(source));
        if (source.endsWith(".json")) {
            try {
                TaskDefinitionLanguage.Task[] tasks = new TDLObjectMapper().readValue(sourceReader, TaskDefinitionLanguage.Task[].class);

                if ((prefix == null) && (tasks.length > 1)) {
                    throw new InvalidConfigValueException("Prefix for a multiple-task JSON config must be provided");
                }

                for (TaskDefinitionLanguage.Task task : tasks) {
                    if ((prefix == null) || prefix.equals(task.prefix)) {
                        task.variables = (variables != null) ? new HashMap(variables) : null;

                        return task;
                    }
                }
            } catch (IOException e) {
                throw new InvalidConfigValueException("Invalid or malformed JSON config", e);
            }
        } else {
            ini.load(sourceReader);
        }

        if (ini.isEmpty()) {
            throw new InvalidConfigValueException("Unable to gather configuration from .ini file");
        }

        return PropertiesReader.toTask(prefix, ini, variables);
    }
}
