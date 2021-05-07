/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.config;

import ash.nazg.config.tdl.PropertiesReader;
import ash.nazg.config.tdl.TDLObjectMapper;
import ash.nazg.config.tdl.TaskDefinitionLanguage;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TaskWrapperConfigBuilder extends WrapperConfigBuilder {
    public TaskWrapperConfigBuilder() {
        options = new Options();

        addOption("h", "help", false, "Print a list of command line options and exit");
        addOption("c", "config", true, "Config file (JSON or .ini format)");
        addOption("x", "task", true, "Task prefix in the config file");
        addOption("V", "variables", true, "name=value pairs of substitution variables for the Spark config encoded as Base64");
        addOption("v", "variablesFile", true, "Path to variables file, name=value pairs per each line");
        addOption("l", "local", false, "Run in local mode (its options have no effect otherwise)");
        addOption("m", "driverMemory", true, "Driver memory for local mode, by default Spark uses 1g");
        addOption("u", "sparkUI", false, "Enable Spark UI for local mode, by default it is disabled");
        addOption("L", "localCores", true, "Set cores # for local mode, by default * -- all cores");
        addOption("S", "wrapperStorePath", true, "Path where to store a list of outputs");
        addOption("i", "input", true, "Override for default input path");
        addOption("o", "output", true, "Override for default output path");
    }

    public TaskDefinitionLanguage.Task build(JavaSparkContext context) throws Exception {
        String prefix = getOptionValue("task");

        Properties variables = new Properties();

        String variablesSource = null;
        if (hasOption("V")) {
            variablesSource = new String(Base64.getDecoder().decode(getOptionValue("V")));
        } else if (hasOption("v")) {
            String variablesFile = getOptionValue("v");

            Path sourcePath = new Path(variablesFile);
            String qualifiedPath = sourcePath.getFileSystem(context.hadoopConfiguration()).makeQualified(sourcePath).toString();

            int lastSlash = variablesFile.lastIndexOf('/');
            variablesFile = (lastSlash < 0) ? variablesFile : variablesFile.substring(0, lastSlash);

            variablesSource = context.wholeTextFiles(variablesFile)
                    .filter(t -> t._1.equals(qualifiedPath))
                    .map(Tuple2::_2)
                    .first();
        }
        if (variablesSource != null) {
            variables.load(new StringReader(variablesSource));
        }

        TaskDefinitionLanguage.Task task = null;
        Properties ini = new Properties();

        if (hasOption("c")) {
            String sourceFile = getOptionValue("config");

            Path sourcePath = new Path(sourceFile);
            String qualifiedPath = sourcePath.getFileSystem(context.hadoopConfiguration()).makeQualified(sourcePath).toString();

            int lastSlash = sourceFile.lastIndexOf('/');
            sourceFile = (lastSlash < 0) ? sourceFile : sourceFile.substring(0, lastSlash);

            Reader sourceReader = new StringReader(
                    context.wholeTextFiles(sourceFile)
                            .filter(t -> t._1.equals(qualifiedPath))
                            .map(Tuple2::_2)
                            .first()
            );

            if (sourceFile.endsWith(".json")) {
                try {
                    TaskDefinitionLanguage.Task[] tasks = new TDLObjectMapper().readValue(sourceReader, TaskDefinitionLanguage.Task[].class);

                    if ((prefix == null) && (tasks.length > 1)) {
                        throw new InvalidConfigValueException("Prefix for a multiple-task JSON config must be provided");
                    }

                    for (TaskDefinitionLanguage.Task t : tasks) {
                        if ((prefix == null) || prefix.equals(t.prefix)) {
                            if (t.variables != null) {
                                t.variables.putAll(new HashMap(variables));
                            } else {
                                t.variables = new HashMap(variables);
                            }

                            task = t;
                            break;
                        }
                    }
                } catch (IOException e) {
                    throw new InvalidConfigValueException("Invalid or malformed JSON config", e);
                }
            } else {
                ini.load(sourceReader);
            }
        } else {
            Stream<Tuple2<String, String>> stream = Arrays.stream(context.getConf().getAll());
            if (prefix != null) {
                stream = stream.filter(t -> t._1.startsWith(prefix));
            }
            Map fromCtx = stream.collect(Collectors.toMap(Tuple2::_1, Tuple2::_2));

            ini.putAll(fromCtx);
        }

        if (task == null) {
            if (ini.isEmpty()) {
                throw new InvalidConfigValueException("Unable to gather configuration from Spark context or .ini file");
            }

            task = PropertiesReader.toTask(prefix, ini, variables);
        }

        return task;
    }

    public void foreignLayerVariable(TaskDefinitionLanguage.Task config, String layer, String option) {
        if (commandLine.hasOption(option)) {
            config.setForeignLayer(layer, commandLine.getOptionValue(option));
        }
    }
}
