/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.config;

import ash.nazg.config.tdl.TaskDescriptionLanguage;

import java.lang.reflect.Constructor;
import java.util.*;
import java.util.regex.Matcher;

public class OperationConfig extends PropertiesConfig {
    public static final String OP_INPUTS_PREFIX = "op.inputs.";
    public static final String OP_INPUT_PREFIX = "op.input.";
    public static final String OP_OUTPUTS_PREFIX = "op.outputs.";
    public static final String OP_OUTPUT_PREFIX = "op.output.";
    public static final String OP_DEFINITION_PREFIX = "op.definition.";
    public static final String COLUMN_SUFFIX = ".column";
    public static final String COLUMNS_SUFFIX = ".columns";

    public final String name;

    public final List<String> inputs = new ArrayList<>();
    public final List<String> outputs = new ArrayList<>();

    public final Map<String, String> namedInputs = new HashMap<>();
    public final Map<String, String> namedOutputs = new HashMap<>();

    public final TypedMap<String, Object> defs = new TypedMap<>();

    private TaskDescriptionLanguage.Operation opDesc;

    public OperationConfig(TaskDescriptionLanguage.Operation opDesc, String name) {
        this.name = name;
        this.opDesc = opDesc;
    }

    @Override
    protected void overrideProperty(String index, String property) {
        String pIndex = replaceVars(index);
        if (pIndex != index) { // yes, String comparison via equality operator is intentional here
            properties.remove(index);
            index = pIndex;
        }

        properties.setProperty(index, replaceVars(property));
    }

    private String replaceVars(String stringWithVars) {
        Matcher hasRepVar = REP_VAR.matcher(stringWithVars);
        while (hasRepVar.find()) {
            String rep = hasRepVar.group(1);

            String repVar = rep;
            String repDef = null;
            if (rep.contains(REP_SEP)) {
                String[] rd = rep.split(REP_SEP, 2);
                repVar = rd[0];
                repDef = rd[1];
            }

            String val = overrides.getProperty(repVar, repDef);

            if (val != null) {
                stringWithVars = stringWithVars.replace("{" + rep + "}", val);
            }
        }

        return stringWithVars;
    }

    public DataStreamsConfig configure(Properties sourceConfig, Properties overrides) throws InvalidConfigValueException {
        setOverrides(overrides);
        setProperties(sourceConfig);

        Set<String> allInputs = new HashSet<>(), columnBasedInputs = new HashSet<>();
        Set<String> allOutputs = new HashSet<>(), columnBasedOutputs = new HashSet<>();

        Map<String, String[]> generatedColumns = new HashMap<>();

        if (opDesc.inputs.positional != null) {
            String[] ins = getArray(OP_INPUTS_PREFIX + name);
            if (ins == null) {
                throw new InvalidConfigValueException("No positional inputs were specified for operation '" + name + "'");
            }

            List<String> inpList = Arrays.asList(ins);

            if ((opDesc.inputs.positionalMinCount != null) && (inpList.size() < opDesc.inputs.positionalMinCount)) {
                throw new InvalidConfigValueException("Operation " + name + " must have at least " + opDesc.inputs.positionalMinCount + " positional inputs, but it has " + inpList.size());
            } else if (inpList.isEmpty()) {
                throw new InvalidConfigValueException("Operation " + name + " requires positional input(s), but it hasn't been supplied with");
            }

            inputs.addAll(inpList);
            allInputs.addAll(inpList);

            if (opDesc.inputs.positional.columnBased) {
                columnBasedInputs.addAll(inpList);
            }
        } else if (opDesc.inputs.named != null) {
            for (TaskDescriptionLanguage.NamedStream ns : opDesc.inputs.named) {
                String opInput = getProperty(OP_INPUT_PREFIX + name + "." + ns.name);
                namedInputs.put(ns.name, opInput);
                allInputs.add(opInput);

                if (ns.columnBased) {
                    columnBasedInputs.add(opInput);
                }
            }

            if (namedInputs.isEmpty()) {
                throw new InvalidConfigValueException("Operation " + name + " consumes named input(s), but it hasn't been supplied with one");
            }
        }

        if (opDesc.outputs.positional != null) {
            String[] outs = getArray(OP_OUTPUTS_PREFIX + name);
            if ((outs == null) || (outs.length == 0)) {
                throw new InvalidConfigValueException("No positional outputs were specified for operation '" + name + "'");
            }

            List<String> outpList = Arrays.asList(outs);

            outputs.addAll(outpList);
            allOutputs.addAll(outpList);
            if (opDesc.outputs.positional.columnBased) {
                columnBasedOutputs.addAll(outpList);

                if (opDesc.outputs.positional.generatedColumns != null) {
                    for (String opOutput : outpList) {
                        generatedColumns.put(opOutput, opDesc.outputs.positional.generatedColumns);
                    }
                }
            }
        } else if (opDesc.outputs.named != null) {
            for (TaskDescriptionLanguage.NamedStream ns : opDesc.outputs.named) {
                String opOutput = getProperty(OP_OUTPUT_PREFIX + name + "." + ns.name);
                namedOutputs.put(ns.name, opOutput);
                allOutputs.add(opOutput);
                if (ns.columnBased) {
                    columnBasedOutputs.add(opOutput);

                    if (ns.generatedColumns != null) {
                        generatedColumns.put(opOutput, ns.generatedColumns);
                    }
                }
            }

            if (namedOutputs.isEmpty()) {
                throw new InvalidConfigValueException("Operation " + name + " produces named output(s), but it hasn't been configured to emit one");
            }
        }

        DataStreamsConfig dsc = new DataStreamsConfig(sourceConfig, allInputs, columnBasedInputs, allOutputs, columnBasedOutputs, generatedColumns);

        if (opDesc.definitions != null) {
            for (TaskDescriptionLanguage.DefBase defDesc : opDesc.definitions) {
                Map<String, String> defProps;
                if (defDesc instanceof TaskDescriptionLanguage.Definition) {
                    TaskDescriptionLanguage.Definition defDe = (TaskDescriptionLanguage.Definition) defDesc;

                    if (defDe.optional) {
                        defProps = Collections.singletonMap(defDe.name, getProperty(OP_DEFINITION_PREFIX + name + "." + defDe.name, defDe.defaults));
                    } else {
                        String definition = getProperty(OP_DEFINITION_PREFIX + name + "." + defDe.name);

                        if (definition == null) {
                            throw new InvalidConfigValueException("Mandatory definition '" + defDe.name + "' of operation '" + name + "' wasn't set");
                        }

                        defProps = Collections.singletonMap(defDe.name, definition);
                    }
                } else {
                    TaskDescriptionLanguage.DynamicDef dynDef = (TaskDescriptionLanguage.DynamicDef) defDesc;

                    Map<String, String> ret = new HashMap<>();
                    String partKey = OP_DEFINITION_PREFIX + name + ".";
                    String fullKey = partKey + dynDef.prefix;
                    Properties properties = getProperties();
                    for (Object key : properties.keySet()) {
                        String prop = (String) key;
                        if (prop.startsWith(fullKey)) {
                            ret.put(prop.substring(partKey.length()), getProperty(prop));
                        }
                    }
                    defProps = ret;
                }

                Class clazz = defDesc.clazz;

                for (Map.Entry<String, String> defProp : defProps.entrySet()) {
                    String name = defProp.getKey();
                    String prop = defProp.getValue();

                    if (prop == null) {
                        defs.put(name, null);
                        continue;
                    }

                    if (Number.class.isAssignableFrom(clazz)) {
                        try {
                            Constructor c = clazz.getConstructor(String.class);
                            defs.put(name, c.newInstance(prop));
                        } catch (Exception e) {
                            throw new InvalidConfigValueException("Bad value '" + prop + "' for '" + clazz.getSimpleName() + "' property '" + name + "' of operation '" + name + "'");
                        }
                    } else if (String.class == clazz) {
                        defs.put(name, prop);
                    } else if (Boolean.class == clazz) {
                        defs.put(name, Boolean.valueOf(prop));
                    } else if (clazz.isEnum()) {
                        defs.put(name, Enum.valueOf(clazz, prop));
                    } else if (String[].class == clazz) {
                        defs.put(name, Arrays.stream(prop.split(COMMA)).map(String::trim).toArray(String[]::new));
                    } else {
                        throw new InvalidConfigValueException("Unknown type of property '" + name + "' of operation '" + name + "'");
                    }
                }
            }
        }

        for (String def : defs.keySet()) {
            if (columnBasedInputs.size() > 0) {
                String[] cols = new String[0];
                Object d = defs.get(def);
                if (def.endsWith(COLUMN_SUFFIX) && (d != null)) {
                    cols = new String[]{(String) d};
                }
                if (def.endsWith(COLUMNS_SUFFIX)) {
                    cols = (String[]) d;
                }
                if (cols != null) {
                    for (String col : cols) {
                        String[] column = col.split("\\.", 2);

                        String[] raw = dsc.inputColumnsRaw.get(column[0]);
                        if (raw == null) {
                            throw new InvalidConfigValueException("Property '" + def + "' of operation '" + name + "' refers to columns of input '" + column[0] + "' but these weren't defined");
                        }
                        if (!column[1].endsWith("*") && !Arrays.asList(raw).contains(column[1])) {
                            throw new InvalidConfigValueException("Property '" + def + "' of operation '" + name + "' refers to inexistent column '" + column[1] + "' of input '" + column[0] + "'");
                        }
                    }
                }
            }
        }

        return dsc;
    }

    public static class TypedMap<K, V> extends HashMap<K, V> {
        public <T extends V> T getTyped(K key) {
            return (T) get(key);
        }
    }
}
