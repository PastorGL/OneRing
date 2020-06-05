/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.composer;

import ash.nazg.composer.config.ComposeWrapperConfigBuilder;
import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.PropertiesConfig;
import ash.nazg.config.WrapperConfig;
import ash.nazg.config.tdl.PropertiesConverter;
import ash.nazg.config.tdl.TDLObjectMapper;
import ash.nazg.config.tdl.TaskDefinitionLanguage;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;

import javax.ws.rs.core.MultivaluedHashMap;
import java.io.FileReader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

import static ash.nazg.cli.TaskWrapper.APP_NAME;

public class Composer {
    public static void main(String... args) {
        ComposeWrapperConfigBuilder configBuilder = new ComposeWrapperConfigBuilder();

        try {
            configBuilder.addRequiredOption("C", "configs", true, "Config files path=alias pairs, separated by a comma");
            configBuilder.addOption("X", "tasks", true, "Task prefix(es), if needed. If each is different, specify all as a comma-separated list. If all are same, specify it only once");
            configBuilder.addOption("V", "variables", true, "name=value pairs of substitution variables for the Spark config, separated by a newline and encoded to Base64");
            configBuilder.addOption("v", "variablesFile", true, "Path to variables file, name=value pairs per each line");
            configBuilder.addRequiredOption("M", "mapping", true, "Data stream mapping file");
            configBuilder.addRequiredOption("o", "output", true, "Full path to the composed output config file (for JSON format use .json extension)");
            configBuilder.addOption("F", "full", false, "Perform full compose (tee outputs only from the last config in the chain)");

            configBuilder.setCommandLine(args);

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

            final Properties overrides = new Properties();
            try {
                String variables = configBuilder.getOptionValue("variables");
                if (variables != null) {
                    variables = new String(Base64.getDecoder().decode(variables));

                    overrides.load(new StringReader(variables));
                } else {
                    final String variablesFile = configBuilder.getOptionValue("variablesFile");
                    overrides.load(new FileReader(variablesFile));
                }
            } catch (Exception ignored) {
                // no variables
            }

            boolean hasUnresolved = false;

            Map<String, TaskDefinitionLanguage.Task> tasksToCompose = new LinkedHashMap<>();
            for (int i = 0; i < configs.length; i++) {
                String[] cfg = configs[i].split("=|\\s+", 2);
                String path = cfg[0];
                String alias = cfg[1];

                String taskPrefix = prefix != null ? prefixes[i] : null;
                WrapperConfig cwc = configBuilder.build(taskPrefix, path);

                System.out.println("Config: " + path + " aliased as " + alias);
                System.out.println("Inputs: " + String.join(", ", cwc.getInputSink()));
                System.out.println("Outputs: " + String.join(", ", cwc.getTeeOutput()));

                Properties properties = cwc.getLayerProperties();
                final MultivaluedHashMap<String, String> mvv = new MultivaluedHashMap<>();
                for (Map.Entry<Object, Object> prop : properties.entrySet()) {
                    String key = prop.getKey().toString();

                    Matcher hasRepVar = PropertiesConfig.REP_VAR.matcher(prop.getValue().toString());
                    if (hasRepVar.matches()) {
                        for (int g = 1; g <= hasRepVar.groupCount(); g++) {
                            String rep = hasRepVar.group(g);

                            String repVar = rep;
                            String repDef = null;
                            if (rep.contains(PropertiesConfig.REP_SEP)) {
                                String[] rd = rep.split(PropertiesConfig.REP_SEP, 2);
                                repVar = rd[0];
                                repDef = rd[1];
                            }

                            if (overrides.containsKey(repVar)) {
                                key += " resolved to " + overrides.getProperty(repVar);
                            } else if (repDef != null) {
                                key += " defaulted to " + repDef;
                            } else {
                                hasUnresolved = true;
                                key += " left unresolved";
                            }

                            mvv.add(repVar, key);
                        }
                    }
                }

                if (!mvv.isEmpty()) {
                    mvv.forEach((key, value) -> System.out.println("Variable {" + key + "} in " + String.join(", ", value)));
                }

                cwc = new WrapperConfig();
                cwc.setOverrides(overrides);
                cwc.setProperties(properties);
                tasksToCompose.put(alias, PropertiesConverter.toTask(cwc));
            }

            if (hasUnresolved) {
                throw new InvalidConfigValueException("There are some unresolved variables. See in the log above");
            }

            String mapping = configBuilder.getOptionValue("mapping");
            final Map<String, Map<String, String>> dsMergeMap = PropertiesConverter.parseMergeMap(Files.readAllLines(Paths.get(mapping), StandardCharsets.UTF_8));

            TaskDefinitionLanguage.Task composed = PropertiesConverter.composeTasks(tasksToCompose, dsMergeMap, configBuilder.hasOption("full"));

            String outputFile = configBuilder.getOptionValue("output");
            Path outputPath = Paths.get(outputFile);
            if (outputFile.endsWith(".json")) {
                Files.write(outputPath, Collections.singleton(new TDLObjectMapper().writeValueAsString(composed)));
            } else {
                Files.write(outputPath, PropertiesConverter.toProperties(composed).entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(Collectors.toList()));
            }
        } catch (Exception ex) {
            if (ex instanceof ParseException) {
                new HelpFormatter().printHelp(APP_NAME, configBuilder.getOptions());
            } else {
                System.out.println(ex.getMessage());
            }

            System.exit(1);
        }
    }
}
