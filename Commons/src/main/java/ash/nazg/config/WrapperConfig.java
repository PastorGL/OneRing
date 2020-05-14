/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static ash.nazg.config.DataStreamsConfig.DS_INPUT_COLUMNS_PREFIX;
import static ash.nazg.config.DataStreamsConfig.DS_INPUT_DELIMITER_PREFIX;

public class WrapperConfig extends TaskConfig {
    public static final String DS_OUTPUT_PATH = "ds.output.path";
    public static final String DS_INPUT_PATH_PREFIX = "ds.input.path.";
    public static final String DS_OUTPUT_PATH_PREFIX = "ds.output.path.";
    public static final String DS_INPUT_PART_COUNT_PREFIX = "ds.input.part_count.";
    public static final String DS_OUTPUT_PART_COUNT_PREFIX = "ds.output.part_count.";
    public static final String DS_INPUT_SINK_SCHEMA_PREFIX = "ds.input.sink_schema.";

    public static final String DISTCP_PREFIX = "distcp.";
    public static final String INPUT_PREFIX = "input.";
    public static final String OUTPUT_PREFIX = "output.";

    private Map<String, Properties> knownLayers = new HashMap<String, Properties>() {{
        put(OUTPUT_PREFIX, null);
        put(INPUT_PREFIX, null);
        put(DISTCP_PREFIX, null);
    }};

    public final String inputPath(String input) {
        return getProperty(DS_INPUT_PATH_PREFIX + input);
    }

    public String getInputProperty(String key, String fallback) {
        return getKnownLayerProperty(INPUT_PREFIX, key, fallback);
    }

    private String getKnownLayerProperty(String layerPrefix, String key, String fallback) {
        Properties layerProperties = getLayerProperties(layerPrefix);

        return layerProperties.getProperty(key, fallback);
    }

    public Properties getLayerProperties(String layerPrefix) {
        if (!knownLayers.containsKey(layerPrefix)) {
            return null;
        }

        Properties layerProperties = knownLayers.get(layerPrefix);
        if (layerProperties == null) {
            layerProperties = new Properties();

            int length = layerPrefix.length();
            for (Map.Entry<Object, Object> e : getProperties().entrySet()) {
                String prop = (String) e.getKey();
                if (prop.startsWith(layerPrefix)) {
                    layerProperties.setProperty(prop.substring(length), (String) e.getValue());
                }
            }
        }

        return layerProperties;
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
        return getKnownLayerProperty(OUTPUT_PREFIX, key, fallback);
    }

    public String getDsInputPartCount(String input) {
        return getProperty(DS_INPUT_PART_COUNT_PREFIX + input, "-1");
    }

    public String getDsOutputPartCount(String output) {
        return getProperty(DS_OUTPUT_PART_COUNT_PREFIX + output, "-1");
    }

    public String getDistCpProperty(String key, String fallback) {
        return getKnownLayerProperty(DISTCP_PREFIX, key, fallback);
    }

    public String[] getSinkSchema(String sink) {
        return getArray(DS_INPUT_SINK_SCHEMA_PREFIX + sink);
    }

    public String[] getSinkColumns(String sink) {
        return getArray(DS_INPUT_COLUMNS_PREFIX + sink);
    }

    public char getSinkDelimiter(String sink) {
        String delimiter = getProperty(DS_INPUT_DELIMITER_PREFIX + sink);

        return ((delimiter == null) || delimiter.isEmpty())
                ? getDsInputDelimiter()
                : delimiter.charAt(0);
    }
}
