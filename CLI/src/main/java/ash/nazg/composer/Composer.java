/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.composer;

import ash.nazg.composer.config.ComposeWrapperConfigBuilder;
import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.PropertiesWriter;
import ash.nazg.config.tdl.TDLObjectMapper;
import ash.nazg.config.tdl.TaskDefinitionLanguage;
import org.apache.commons.cli.ParseException;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class Composer {
    public static void main(String... args) {
        ComposeWrapperConfigBuilder configBuilder = new ComposeWrapperConfigBuilder();

        try {
            configBuilder.setCommandLine(args, "Composer");

            String prefix = configBuilder.getOptionValue("tasks");
            String[] prefixes = null;
            if (prefix != null) {
                prefixes = prefix.split(",");
            }

            String config = configBuilder.getOptionValue("configs");
            String[] configs = config.split(",");

            if (configs.length < 2) {
                throw new InvalidConfigValueException("Insufficient number of configuration file paths to compose");
            }

            if (prefix != null) {
                if (prefixes.length > 1) {
                    if (prefixes.length != configs.length) {
                        throw new InvalidConfigValueException("Task prefixes '" + prefix + "' must be specified for all " + configs.length + " config(s)");
                    }
                } else {
                    prefixes = new String[configs.length];
                    Arrays.fill(prefixes, prefix);
                }
            }

            final Properties variables = new Properties();
            try {
                String variablesSource = configBuilder.getOptionValue("variables");
                if (variablesSource != null) {
                    variablesSource = new String(Base64.getDecoder().decode(variablesSource));

                    variables.load(new StringReader(variablesSource));
                } else {
                    final String variablesFile = configBuilder.getOptionValue("variablesFile");
                    variables.load(new FileReader(variablesFile));
                }
            } catch (Exception ignored) {
                // no variables
            }

            Map<String, TaskDefinitionLanguage.Task> tasksToCompose = new LinkedHashMap<>();
            for (int i = 0; i < configs.length; i++) {
                String[] cfg = configs[i].split("=|\\s+", 2);
                String path = cfg[0];
                String alias = cfg[1];

                String taskPrefix = prefix != null ? prefixes[i] : null;
                TaskDefinitionLanguage.Task cwc = configBuilder.build(taskPrefix, path, variables);

                tasksToCompose.put(alias, cwc);
            }

            String mapping = configBuilder.getOptionValue("mapping");
            final Map<String, Map<String, String>> dsMergeMap = ash.nazg.config.tdl.Composer.parseMergeMap(Files.readAllLines(Paths.get(mapping), StandardCharsets.UTF_8));

            TaskDefinitionLanguage.Task composed = ash.nazg.config.tdl.Composer.composeTasks(tasksToCompose, dsMergeMap, configBuilder.hasOption("full"));

            String outputFile = configBuilder.getOptionValue("output");
            if (outputFile.endsWith(".json")) {
                try (Writer jsonWriter = new BufferedWriter(new FileWriter(outputFile))) {
                    jsonWriter.write(new TDLObjectMapper().writeValueAsString(composed));
                }
            } else {
                try (Writer iniWriter = new BufferedWriter(new FileWriter(outputFile))) {
                    PropertiesWriter.writeProperties(composed, iniWriter);
                }
            }
        } catch (Exception ex) {
            if (ex instanceof ParseException) {
                configBuilder.printHelp("Composer");
            } else {
                System.out.println(ex.getMessage());
            }

            System.exit(1);
        }
    }
}
