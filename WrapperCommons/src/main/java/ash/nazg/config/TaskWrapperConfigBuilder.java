/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.config;

import ash.nazg.storage.Adapters;
import org.apache.commons.cli.Options;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.FileReader;
import java.io.StringReader;
import java.util.Base64;
import java.util.Properties;

public class TaskWrapperConfigBuilder extends WrapperConfigBuilder {
    public TaskWrapperConfigBuilder() {
        options = new Options()
                .addOption("x", "task", true, "Task prefix in the config file")
                .addOption("V", "variables", true, "name=value pairs of substitution variables for the Spark config encoded as Base64")
                .addOption("v", "variablesFile", true, "Path to variables file, name=value pairs per each line")
                .addOption("l", "local", false, "Run in local[*] mode")
                .addOption("m", "memory", true, "Driver memory for local mode (no effect otherwise)")
                .addOption("S", "wrapperStorePath", true, "Path where to store a list of wrapped wildcards outputs");
    }

    public WrapperConfig build(JavaSparkContext context) {
        String prefix = getOptionValue("task");
        if (prefix != null) {
            System.out.println("Task prefix");
            System.out.println(prefix);
        } else {
            System.out.println("Non-prefixed task");
        }

        wrapperConfig = new WrapperConfig();
        wrapperConfig.setPrefix(prefix);

        Properties overrides = new Properties();
        try {
            String variables = getOptionValue("variables");
            if (variables != null) {
                variables = new String(Base64.getDecoder().decode(variables));

                overrides.load(new StringReader(variables));
            } else {
                final String variablesFile = getOptionValue("variablesFile");
                if (Adapters.PATH_PATTERN.matcher(variablesFile).matches() && (context != null)) {
                    variables = context.wholeTextFiles(variablesFile.substring(0, variablesFile.lastIndexOf('/')))
                            .filter(t -> t._1.equals(variablesFile))
                            .map(t -> t._2)
                            .first();
                    overrides.load(new StringReader(variables));
                } else {
                    overrides.load(new FileReader(variablesFile));
                }
            }

            System.out.println("Collected overrides");
            overrides.forEach((key, value) -> System.out.println(key + "=" + value));
            wrapperConfig.setOverrides(overrides);
        } catch (Exception ignored) {
            System.out.println("No overrides collected");
        }

        Properties ini = loadConfig(getOptionValue("config"), context, prefix);
        if (ini.isEmpty()) {
            throw new InvalidConfigValueException("Configuration source is empty and Spark context doesn't have expected task properties");
        }

        System.out.println("Collected properties");
        ini.forEach((key, value) -> System.out.println(key + "=" + value));
        wrapperConfig.setProperties(ini);

        return wrapperConfig;
    }

    public void overrideFromCommandLine(String index, String option) {
        if (commandLine.hasOption(option)) {
            wrapperConfig.overrideProperty(index, commandLine.getOptionValue(option));
        }
    }
}
