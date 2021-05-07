/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.config.tdl;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.spark.OpInfo;
import ash.nazg.spark.Operations;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

public class PropertiesReader {
    static public TaskDefinitionLanguage.Task toTask(Properties source, Properties variables) {
        return toTask(null, source, variables);
    }

    static public TaskDefinitionLanguage.Task toTask(String prefix, Properties source, Properties variables) {
        TaskDefinitionLanguage.Task task = TaskDefinitionLanguage.createTask();

        task.variables = (variables != null) ? new HashMap(variables) : null;

        Map<Object, Object> taskConfig;

        if (prefix == null) {
            for (Object k : source.keySet()) {
                String key = (String) k;
                if (key.endsWith("." + Constants.TASK_OPERATIONS)) {
                    String rawPrefix = key.substring(0, key.length() - Constants.TASK_OPERATIONS.length());

                    task.prefix = rawPrefix.substring(0, rawPrefix.length() - 1);

                    break;
                }
            }
        } else {
            task.prefix = prefix;
        }

        if (task.prefix == null) {
            taskConfig = source;
        } else {
            final int prefixLen = task.prefix.length();

            taskConfig = source.entrySet().stream()
                    .filter(e -> ((String) e.getKey()).startsWith(task.prefix))
                    .collect(Collectors.toMap(e -> ((String) e.getKey()).substring(prefixLen + 1), Map.Entry::getValue));
        }

        Map<String, OpInfo> ao = Operations.availableOperations;

        List<String> taskItems = getOperations(taskConfig);

        for (String taskItem : taskItems) {
            TaskDefinitionLanguage.TaskItem tiDef;

            if (!taskItem.startsWith(Constants.DIRECTIVE_SIGIL)) {
                String verb = getProperty(taskConfig, Constants.OP_OPERATION_PREFIX + taskItem);

                if (!ao.containsKey(verb)) {
                    throw new InvalidConfigValueException("Operation '" + taskItem + "' has unknown verb '" + verb + "'");
                }

                OpInfo chainedOp = ao.get(verb);

                TaskDescriptionLanguage.Operation opDesc = chainedOp.description;
                TaskDefinitionLanguage.Operation opDef = TaskDefinitionLanguage.createOperation(task);

                opDef.name = taskItem;
                opDef.verb = verb;

                if (opDesc.definitions != null) {
                    opDef.definitions = TaskDefinitionLanguage.createDefinitions(task);

                    for (TaskDescriptionLanguage.DefBase defDesc : opDesc.definitions.values()) {
                        if (defDesc instanceof TaskDescriptionLanguage.DynamicDef) {
                            TaskDescriptionLanguage.DynamicDef dynDef = (TaskDescriptionLanguage.DynamicDef) defDesc;
                            String dp = Constants.OP_DEFINITION_PREFIX + taskItem + '.';
                            int pl = dp.length();
                            dp += dynDef.name;
                            for (Map.Entry e : taskConfig.entrySet()) {
                                String key = (String) e.getKey();
                                if (key.startsWith(dp)) {
                                    String dynDefName = key.substring(pl);
                                    opDef.definitions.put(dynDefName, (String) e.getValue());
                                }
                            }
                        } else {
                            TaskDescriptionLanguage.Definition defDe = (TaskDescriptionLanguage.Definition) defDesc;

                            if (defDe.optional) {
                                opDef.definitions.put(defDe.name, getProperty(taskConfig, Constants.OP_DEFINITION_PREFIX + taskItem + "." + defDe.name, defDe.defaults));
                            } else {
                                String definition = getProperty(taskConfig, Constants.OP_DEFINITION_PREFIX + taskItem + "." + defDe.name);

                                if (definition == null) {
                                    throw new InvalidConfigValueException("Mandatory definition '" + defDe.name + "' of operation '" + taskItem + "' wasn't set");
                                }

                                opDef.definitions.put(defDe.name, definition);
                            }
                        }
                    }
                }

                if (opDesc.inputs.positional != null) {
                    String[] posInputs = getArray(taskConfig, Constants.OP_INPUTS_PREFIX + taskItem);
                    if (posInputs == null) {
                        throw new InvalidConfigValueException("No positional inputs were specified for operation '" + taskItem + "'");
                    }

                    opDef.inputs.positionalNames = String.join(Constants.COMMA, posInputs);
                    for (String inp : posInputs) {
                        if (inp != null) {
                            task.dataStreams.compute(inp, (k, ds) -> {
                                if (ds == null) {
                                    ds = new TaskDefinitionLanguage.DataStream();
                                }
                                return ds;
                            });
                        }
                    }
                } else if (opDesc.inputs.named != null) {
                    opDef.inputs.named = TaskDefinitionLanguage.createDefinitions(task);
                    for (int i = 0; i < opDesc.inputs.named.length; i++) {
                        TaskDescriptionLanguage.NamedStream inDesc = opDesc.inputs.named[i];
                        String inp = getProperty(taskConfig, Constants.OP_INPUT_PREFIX + taskItem + "." + inDesc.name);

                        if (inp != null) {
                            opDef.inputs.named.put(inDesc.name, inp);

                            task.dataStreams.compute(inp, (k, ds) -> {
                                if (ds == null) {
                                    ds = new TaskDefinitionLanguage.DataStream();
                                }
                                return ds;
                            });
                        }
                    }

                    if (opDef.inputs.named.isEmpty()) {
                        throw new InvalidConfigValueException("Operation " + taskItem + " consumes named input(s), but it hasn't been supplied with one");
                    }
                }

                if (opDesc.outputs.positional != null) {
                    String[] posOutputs = getArray(taskConfig, Constants.OP_OUTPUTS_PREFIX + taskItem);

                    if ((posOutputs == null) || (posOutputs.length == 0)) {
                        throw new InvalidConfigValueException("No positional outputs were specified for operation '" + taskItem + "'");
                    }

                    opDef.outputs.positionalNames = String.join(Constants.COMMA, posOutputs);
                    for (String outp : posOutputs) {
                        task.dataStreams.compute(outp, (k, ds) -> {
                            if (ds == null) {
                                ds = new TaskDefinitionLanguage.DataStream();
                            }
                            return ds;
                        });
                    }
                } else if (opDesc.outputs.named != null) {
                    opDef.outputs.named = TaskDefinitionLanguage.createDefinitions(task);
                    for (int i = 0; i < opDesc.outputs.named.length; i++) {
                        TaskDescriptionLanguage.NamedStream outDesc = opDesc.outputs.named[i];
                        String outp = getProperty(taskConfig, Constants.OP_OUTPUT_PREFIX + taskItem + "." + outDesc.name);
                        if (outp != null) {
                            opDef.outputs.named.put(outDesc.name, outp);

                            task.dataStreams.compute(outp, (k, ds) -> {
                                if (ds == null) {
                                    ds = new TaskDefinitionLanguage.DataStream();
                                }
                                return ds;
                            });
                        }
                    }

                    if (opDef.outputs.named.isEmpty()) {
                        throw new InvalidConfigValueException("Operation " + taskItem + " produces named output(s), but it hasn't been configured to emit one");
                    }
                }

                tiDef = opDef;
            } else {
                TaskDefinitionLanguage.Directive dirDef = TaskDefinitionLanguage.createDirective(task);

                dirDef.directive = taskItem;

                tiDef = dirDef;
            }

            task.taskItems.add(tiDef);
        }

        String[] input = getArray(taskConfig, Constants.TASK_INPUT);
        if (input != null) {
            Collections.addAll(task.input, input);
        }

        String[] outputs = getArray(taskConfig, Constants.TASK_OUTPUT);
        if (outputs != null) {
            Collections.addAll(task.output, outputs);
        }

        Set<String> dataStreams = new HashSet<>();
        for (Map.Entry e : taskConfig.entrySet()) {
            String key = (String) e.getKey();
            if (key.startsWith(Constants.DS_INPUT_PREFIX) || key.startsWith(Constants.DS_OUTPUT_PREFIX)) {
                String[] ds = key.split("\\.", 4);
                if (ds.length == 4) {
                    dataStreams.add(ds[3]);
                }
            }
        }

        dataStreams.forEach(dsName -> task.dataStreams.compute(dsName, (k, ds) -> {
            if (ds == null) {
                ds = new TaskDefinitionLanguage.DataStream();
            }

            ds.input = new TaskDefinitionLanguage.StreamDesc();
            ds.input.columns = getProperty(taskConfig, Constants.DS_INPUT_COLUMNS_PREFIX + dsName);
            ds.input.delimiter = getProperty(taskConfig, Constants.DS_INPUT_DELIMITER_PREFIX + dsName);
            ds.input.partCount = getProperty(taskConfig, Constants.DS_INPUT_PART_COUNT_PREFIX + dsName);
            ds.input.path = getProperty(taskConfig, Constants.DS_INPUT_PATH_PREFIX + dsName);

            ds.output = new TaskDefinitionLanguage.StreamDesc();
            ds.output.columns = getProperty(taskConfig, Constants.DS_OUTPUT_COLUMNS_PREFIX + dsName);
            ds.output.delimiter = getProperty(taskConfig, Constants.DS_OUTPUT_DELIMITER_PREFIX + dsName);
            ds.output.partCount = getProperty(taskConfig, Constants.DS_OUTPUT_PART_COUNT_PREFIX + dsName);
            ds.output.path = getProperty(taskConfig, Constants.DS_OUTPUT_PATH_PREFIX + dsName);

            return ds;
        }));
        TaskDefinitionLanguage.DataStream defaultDs = task.dataStreams.get(Constants.DEFAULT_DS);
        if (StringUtils.isEmpty(defaultDs.input.path)) {
            defaultDs.input.path = getProperty(taskConfig, Constants.DS_INPUT_PATH);
        }
        if (StringUtils.isEmpty(defaultDs.input.delimiter)) {
            defaultDs.input.delimiter = getProperty(taskConfig, Constants.DS_INPUT_DELIMITER);
        }
        if (StringUtils.isEmpty(defaultDs.output.path)) {
            defaultDs.output.path = getProperty(taskConfig, Constants.DS_OUTPUT_PATH);
        }
        if (StringUtils.isEmpty(defaultDs.output.delimiter)) {
            defaultDs.output.delimiter = getProperty(taskConfig, Constants.DS_OUTPUT_DELIMITER);
        }

        taskConfig.entrySet().stream()
                .filter(e -> {
                    String key = (String) e.getKey();
                    return !key.startsWith("ds.") && !key.startsWith("op.") && !key.startsWith("task.");
                })
                .forEach(e -> task.setForeignLayer((String) e.getKey(), (String) e.getValue()));

        return task;
    }

    private static String[] getArray(Map props, String key) throws InvalidConfigValueException {
        String property = getProperty(props, key);

        if (StringUtils.isEmpty(property)) {
            return null;
        }

        String[] strings = Arrays.stream(property.split(Constants.COMMA)).map(String::trim).filter(s -> !s.isEmpty()).toArray(String[]::new);
        return (strings.length == 0) ? null : strings;
    }

    private static String getProperty(Map props, String index) {
        return (String) props.get(index);
    }

    private static String getProperty(Map props, String index, String defaultValue) {
        return (String) props.getOrDefault(index, defaultValue);
    }

    private static List<String> getOperations(Map props) {
        List<String> opNames = new ArrayList<>();
        String names = getProperty(props, Constants.TASK_OPERATIONS);
        if (names != null) {
            Matcher ops = Constants.REP_OPERATIONS.matcher(names);
            while (ops.find()) {
                String opName = ops.group(1).trim();
                if (!opName.startsWith(Constants.DIRECTIVE_SIGIL) && opNames.contains(opName)) {
                    throw new InvalidConfigValueException("Duplicate operation '" + opName + "' in the operations list was encountered for the task");
                }

                opNames.add(opName);
            }
        } else {
            throw new InvalidConfigValueException("Operations were not specified for the task");
        }

        return opNames;
    }
}
