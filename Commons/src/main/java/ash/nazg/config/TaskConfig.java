/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.config;

import ash.nazg.config.tdl.DirVarVal;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TaskConfig extends PropertiesConfig {
    public static final String DIRECTIVE_SIGIL = "$";
    public static final Pattern REP_OPERATIONS = Pattern.compile("(\\$[^{,]+\\{[^}]+?}|\\$[^,]+|[^,]+)");

    public static final String TASK_INPUT_SINK = "task.input.sink";
    public static final String TASK_TEE_OUTPUT = "task.tee.output";
    public static final String TASK_OPERATIONS = "task.operations";
    public static final String OP_OPERATION_PREFIX = "op.operation.";

    private List<String> inputSink = null;
    private List<String> teeOutput = null;
    private List<String> opNames = null;

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
    public Properties getProperties() {
        return super.getProperties();
    }

    @Override
    public Properties getOverrides() {
        return super.getOverrides();
    }

    @Override
    public void setProperties(Properties source) {
        super.setProperties(source);
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
}
