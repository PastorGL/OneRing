package ash.nazg.cli.config;

import ash.nazg.config.PropertiesConfig;
import org.apache.commons.cli.Options;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.io.StringReader;
import java.util.Base64;
import java.util.Properties;

public class TaskWrapperConfigBuilder extends WrapperConfigBuilder {
    public TaskWrapperConfigBuilder() {
        options = new Options()
                .addOption("x", "task", true, "Task prefix in the config file")
                .addOption("V", "variables", true, "name=value pairs of substitution variables for the Spark config encoded as Base64")
                .addOption("S", "wrapperStorePath", true, "Path for DistWrapper interface file");
    }

    public TaskWrapperConfig build(JavaSparkContext context) {
        String prefix = getOptionValue("task");
        System.out.println("Task prefix");
        System.out.println(prefix);

        wrapperConfig = new TaskWrapperConfig();
        wrapperConfig.setPrefix(prefix);

        Properties overrides = new Properties();
        String variables = getOptionValue("variables");
        if (variables != null) {
            variables = new String(Base64.getDecoder().decode(variables));

            try {
                overrides.load(new StringReader(variables));
            } catch (IOException ignored) {
            }
        }
        System.out.println("Collected overrides");
        overrides.forEach((key, value) -> System.out.println(key + "=" + value));
        wrapperConfig.setOverrides(overrides);

        Properties ini = loadConfig(getOptionValue("config"), context, prefix);
        System.out.println("Collected properties");
        ini.forEach((key, value) -> System.out.println(key + "=" + value));
        wrapperConfig.setProperties(ini);

        return (TaskWrapperConfig) wrapperConfig;
    }

    /**
     * Set a {@link PropertiesConfig} parameter directly from a specified command line argument
     *
     * @param index  parameter
     * @param option command line argument
     */
    public void overrideFromCommandLine(String index, String option) {
        if (commandLine.hasOption(option)) {
            wrapperConfig.overrideProperty(index, commandLine.getOptionValue(option));
        }
    }
}