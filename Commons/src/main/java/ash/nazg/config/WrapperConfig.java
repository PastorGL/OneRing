/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.config;

import ash.nazg.config.tdl.DirVarVal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WrapperConfig extends PropertiesConfig {
    public static final String DS_OUTPUT_PATH = "ds.output.path";
    public static final String DS_INPUT_PATH_PREFIX = "ds.input.path.";
    public static final String DS_OUTPUT_PATH_PREFIX = "ds.output.path.";
    public static final String DS_INPUT_PART_COUNT_PREFIX = "ds.input.part_count.";
    public static final String DS_OUTPUT_PART_COUNT_PREFIX = "ds.output.part_count.";
    public static final String DS_INPUT_SINK_SCHEMA_PREFIX = "ds.input.sink_schema.";

    public static final String DISTCP_PREFIX = "distcp.";
    public static final String INPUT_PREFIX = "input.";
    public static final String OUTPUT_PREFIX = "output.";
    public static final String TASK_PREFIX = "task.";
    public static final String OP_PREFIX = "op.";
    public static final String DS_PREFIX = "ds.";

    public static final String TASK_METRICS_PREFIX = "task.metrics.";

    public static final String TASK_INPUT_SINK = "task.input.sink";
    public static final String TASK_TEE_OUTPUT = "task.tee.output";
    public static final String TASK_OPERATIONS = "task.operations";

    public static final String DIRECTIVE_SIGIL = "$";
    public static final Pattern REP_OPERATIONS = Pattern.compile("(\\$[^{,]+\\{[^}]+?}|\\$[^,]+|[^,]+)");
    public static final String OP_OPERATION_PREFIX = "op.operation.";

    private static final List<String> KNOWN_PREFIXES = Arrays.asList(DISTCP_PREFIX, INPUT_PREFIX, OUTPUT_PREFIX, TASK_PREFIX, OP_PREFIX, DS_PREFIX);

    private List<String> inputSink = null;
    private List<String> teeOutput = null;
    private List<String> opNames = null;

    private DataStreamsConfig dsConfig = null;

    public List<String> getInputSink() {
        if (inputSink == null) {
            inputSink = new ArrayList<>();
            String[] sinks = getArray(TASK_INPUT_SINK);
            if (sinks != null) {
                inputSink.addAll(Arrays.asList(sinks));
            }
        }

        return inputSink;
    }

    public List<String> getTeeOutput() {
        if (teeOutput == null) {
            teeOutput = new ArrayList<>();
            String[] tees = getArray(TASK_TEE_OUTPUT);
            if (tees != null) {
                teeOutput.addAll(Arrays.asList(tees));
            }
        }

        return teeOutput;
    }

    @Override
    public Properties getOverrides() {
        return super.getOverrides();
    }

    @Override
    public String getPrefix() {
        String prefix = super.getPrefix();
        return (prefix == null) ? null : prefix.substring(0, prefix.length() - 1);
    }

    @Override
    public void setPrefix(String prefix) {
        super.setPrefix(prefix);
    }

    public String getVerb(String opName) {
        String verb = getProperty(OP_OPERATION_PREFIX + opName);
        if (verb == null) {
            throw new InvalidConfigValueException("Operation named '" + opName + "' has no verb set in the task");
        }

        return verb;
    }

    public DirVarVal getDirVarVal(String directive) {
        DirVarVal dvv;

        Matcher hasRepVar = REP_VAR.matcher(directive);
        if (hasRepVar.find()) {
            String rep = hasRepVar.group(1);

            String repVar = rep;
            String repDef = null;
            if (rep.contains(REP_SEP)) {
                String[] rd = rep.split(REP_SEP, 2);
                repVar = rd[0];
                repDef = rd[1];
            }

            String val = getOverrides().getProperty(repVar, repDef);

            dvv = new DirVarVal(directive.substring(1, hasRepVar.start(1) - 1), repVar, val);
        } else {
            dvv = new DirVarVal(directive.substring(1));
        }

        return dvv;
    }

    public List<String> getOperations() {
        if (opNames == null) {
            opNames = new ArrayList<>();
            String names = getProperty(TASK_OPERATIONS);
            if (names != null) {
                Matcher ops = REP_OPERATIONS.matcher(names);
                while (ops.find()) {
                    String opName = ops.group(1).trim();
                    if (!opName.startsWith(DIRECTIVE_SIGIL) && opNames.contains(opName)) {
                        throw new InvalidConfigValueException("Duplicate operation '" + opName + "' in the operations list was encountered for the task");
                    }

                    opNames.add(opName);
                }
            } else {
                throw new InvalidConfigValueException("Operations were not specified for the task");
            }
        }

        return opNames;
    }

    public final String inputPath(String input) {
        return getDataStreamsConfig().getProperty(DS_INPUT_PATH_PREFIX + input);
    }

    public String getInputProperty(String key, String input, String fallback) {
        return getLayerProperty(INPUT_PREFIX, key, input, fallback);
    }

    private String getLayerProperty(String layerPrefix, String key, String keySuffix, String fallback) {
        Properties layerProperties = getLayerProperties(layerPrefix);

        String defaultVal = layerProperties.getProperty(layerPrefix + key, fallback);
        return (keySuffix == null) ? defaultVal : layerProperties.getProperty(layerPrefix + key + "." + keySuffix, defaultVal);
    }

    public Properties getLayerProperties(String... layerPrefixes) {
        Properties layerProperties = new Properties();

        List<String> prefixes;
        if ((layerPrefixes == null) || (layerPrefixes.length == 0)) {
            prefixes = KNOWN_PREFIXES;
        } else {
            prefixes = new ArrayList<>();

            kp:
            for (String kp : KNOWN_PREFIXES) {
                for (String prefix : layerPrefixes) {
                    if (prefix.startsWith(kp)) {
                        prefixes.add(prefix);
                        continue kp;
                    }
                }
            }
        }

        for (String layerPrefix : prefixes) {
            boolean replace = !layerPrefix.startsWith(TASK_PREFIX);

            Properties thisLayerProperties = getLayerProperties(layerPrefix, replace);

            layerProperties.putAll(thisLayerProperties);
        }

        return layerProperties;
    }

    public final int inputParts(String input) {
        return Integer.parseInt(getDsInputPartCount(input));
    }

    public final int outputParts(String output) {
        return Integer.parseInt(getDsOutputPartCount(output));
    }

    public final String defaultOutputPath() {
        return getDataStreamsConfig().getProperty(DS_OUTPUT_PATH);
    }

    public final String outputPath(String output) {
        String path = getDataStreamsConfig().getProperty(DS_OUTPUT_PATH_PREFIX + output);

        if (path != null) {
            return path;
        }

        path = defaultOutputPath();
        if (path == null) {
            throw new InvalidConfigValueException("Default output path is not configured");
        }

        return path + "/" + output;
    }

    public String getOutputProperty(String key, String output, String fallback) {
        return getLayerProperty(OUTPUT_PREFIX, key, output, fallback);
    }

    public String getDsInputPartCount(String input) {
        return getDataStreamsConfig().getProperty(DS_INPUT_PART_COUNT_PREFIX + input, "-1");
    }

    public String getDsOutputPartCount(String output) {
        return getDataStreamsConfig().getProperty(DS_OUTPUT_PART_COUNT_PREFIX + output, "-1");
    }

    public String getDistCpProperty(String key, String fallback) {
        return getLayerProperty(DISTCP_PREFIX, key, null, fallback);
    }

    private DataStreamsConfig getDataStreamsConfig() {
        if (dsConfig == null) {
            List<String> is = getInputSink();

            dsConfig = new DataStreamsConfig(getLayerProperties(DS_PREFIX), is, is, null, null, null);
        }

        return dsConfig;
    }

    public String[] getSinkSchema(String sink) {
        return getDataStreamsConfig().getArray(DS_INPUT_SINK_SCHEMA_PREFIX + sink);
    }

    public String[] getSinkColumns(String sink) {
        return getDataStreamsConfig().inputColumnsRaw.get(sink);
    }

    public char getSinkDelimiter(String sink) {
        return getDataStreamsConfig().inputDelimiter(sink);
    }

    public String metricsStorePath() {
        return getLayerProperty(TASK_METRICS_PREFIX, "store", null, null);
    }
}
