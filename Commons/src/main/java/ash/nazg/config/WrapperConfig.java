package ash.nazg.config;

import java.util.Properties;

public class WrapperConfig extends TaskConfig {
    public static final String DS_OUTPUT_PATH = "ds.output.path";
    public static final String DS_INPUT_PATH_PREFIX = "ds.input.path.";
    public static final String DS_OUTPUT_PATH_PREFIX = "ds.output.path.";
    public static final String DS_INPUT_PART_COUNT_PREFIX = "ds.input.part_count.";
    public static final String DS_OUTPUT_PART_COUNT_PREFIX = "ds.output.part_count.";

    public static final String OUTPUT_PREFIX = "output.";
    public static final String INPUT_PREFIX = "input.";

    @Override
    public void setOverrides(Properties overrides) {
        super.setOverrides(overrides);
    }

    @Override
    public void overrideProperty(String index, String property) {
        super.overrideProperty(index, property);
    }

    @Override
    public void setProperties(Properties source) {
        super.setProperties(source);
    }

    public final String inputPath(String input) {
        return getProperty(DS_INPUT_PATH_PREFIX + input);
    }

    public String getInputProperty(String key, String fallback) {
        return getProperty(INPUT_PREFIX + key, fallback);
    }

    private String getDsOutputPath() throws InvalidConfigValueException {
        return getProperty(DS_OUTPUT_PATH);
    }

    public final int inputParts(String input) {
        return Integer.parseInt(getDsInputPartCount(input));
    }

    public final int outputParts(String output) {
        return Integer.parseInt(getDsOutputPartCount(output));
    }

    public final String defaultOutputPath() {
        return getDsOutputPath();
    }

    public final String outputPath(String output) {
        String path = getProperty(DS_OUTPUT_PATH_PREFIX + output);

        if (path != null) {
            return path;
        }

        path = getDsOutputPath();
        if (path == null) {
            throw new InvalidConfigValueException("Default output path is not configured");
        }

        return path + "/" + output;
    }

    public String getOutputProperty(String key, String fallback) {
        return getProperty(OUTPUT_PREFIX + key, fallback);
    }

    public String getDsInputPartCount(String input) {
        return getProperty(DS_INPUT_PART_COUNT_PREFIX + input, "-1");
    }

    public String getDsOutputPartCount(String output) {
        return getProperty(DS_OUTPUT_PART_COUNT_PREFIX + output, "-1");
    }
}
