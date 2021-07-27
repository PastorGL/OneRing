/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.config.tdl;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.metadata.DefinitionMeta;
import ash.nazg.config.tdl.metadata.NamedStreamsMeta;
import ash.nazg.config.tdl.metadata.OperationMeta;
import ash.nazg.config.tdl.metadata.PositionalStreamsMeta;
import ash.nazg.spark.OperationInfo;
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

        List<String> taskItems = getOperations(taskConfig);

        for (String taskItem : taskItems) {
            TaskDefinitionLanguage.TaskItem tiDef;

            if (!taskItem.startsWith(Constants.DIRECTIVE_SIGIL)) {
                String verb = getProperty(taskConfig, Constants.OP_OPERATION_PREFIX + taskItem);

                if (!Operations.OPERATIONS.containsKey(verb)) {
                    throw new InvalidConfigValueException("Operation '" + taskItem + "' has unknown verb '" + verb + "'");
                }

                OperationInfo chainedOp = Operations.OPERATIONS.get(verb);

                OperationMeta opDesc = chainedOp.meta;
                TaskDefinitionLanguage.Operation opDef = TaskDefinitionLanguage.createOperation(task);

                opDef.name = taskItem;
                opDef.verb = verb;

                if (opDesc.definitions != null) {
                    opDef.definitions = TaskDefinitionLanguage.createDefinitions(task);

                    for (Map.Entry<String, DefinitionMeta> defMeta : opDesc.definitions.entrySet()) {
                        String name = defMeta.getKey();
                        DefinitionMeta defDesc = defMeta.getValue();

                        if (defDesc.dynamic) {
                            String dp = Constants.OP_DEFINITION_PREFIX + taskItem + '.';
                            int pl = dp.length();
                            dp += name;
                            for (Map.Entry e : taskConfig.entrySet()) {
                                String key = (String) e.getKey();
                                if (key.startsWith(dp)) {
                                    String dynDefName = key.substring(pl);
                                    opDef.definitions.put(dynDefName, (String) e.getValue());
                                }
                            }
                        } else {
                            if (defDesc.optional) {
                                opDef.definitions.put(name, getProperty(taskConfig, Constants.OP_DEFINITION_PREFIX + taskItem + "." + name, (defDesc.defaults == null) ? null : String.valueOf(defDesc.defaults)));
                            } else {
                                String definition = getProperty(taskConfig, Constants.OP_DEFINITION_PREFIX + taskItem + "." + name);

                                if (definition == null) {
                                    throw new InvalidConfigValueException("Mandatory definition '" + name + "' of operation '" + taskItem + "' wasn't set");
                                }

                                opDef.definitions.put(name, definition);
                            }
                        }
                    }
                }

                if (opDesc.input instanceof PositionalStreamsMeta) {
                    String[] posInputs = getArray(taskConfig, Constants.OP_INPUTS_PREFIX + taskItem);
                    if (posInputs == null) {
                        throw new InvalidConfigValueException("No positional inputs were specified for operation '" + taskItem + "'");
                    }

                    opDef.input.positional = String.join(Constants.COMMA, posInputs);
                    for (String inp : posInputs) {
                        if (inp != null) {
                            task.streams.compute(inp, (k, ds) -> {
                                if (ds == null) {
                                    ds = new TaskDefinitionLanguage.DataStream();
                                }
                                return ds;
                            });
                        }
                    }
                } else if (opDesc.input instanceof NamedStreamsMeta) {
                    opDef.input.named = TaskDefinitionLanguage.createDefinitions(task);
                    for (String name : ((NamedStreamsMeta) opDesc.input).streams.keySet()) {
                        String inp = getProperty(taskConfig, Constants.OP_INPUT_PREFIX + taskItem + "." + name);

                        if (inp != null) {
                            opDef.input.named.put(name, inp);

                            task.streams.compute(inp, (k, ds) -> {
                                if (ds == null) {
                                    ds = new TaskDefinitionLanguage.DataStream();
                                }
                                return ds;
                            });
                        }
                    }

                    if (opDef.input.named.isEmpty()) {
                        throw new InvalidConfigValueException("Operation " + taskItem + " consumes named input(s), but it hasn't been supplied with one");
                    }
                }

                if (opDesc.output instanceof PositionalStreamsMeta) {
                    String[] posOutputs = getArray(taskConfig, Constants.OP_OUTPUTS_PREFIX + taskItem);

                    if ((posOutputs == null) || (posOutputs.length == 0)) {
                        throw new InvalidConfigValueException("No positional outputs were specified for operation '" + taskItem + "'");
                    }

                    opDef.output.positional = String.join(Constants.COMMA, posOutputs);
                    for (String outp : posOutputs) {
                        task.streams.compute(outp, (k, ds) -> {
                            if (ds == null) {
                                ds = new TaskDefinitionLanguage.DataStream();
                            }
                            return ds;
                        });
                    }
                } else if (opDesc.output instanceof NamedStreamsMeta) {
                    opDef.output.named = TaskDefinitionLanguage.createDefinitions(task);
                    for (String name : ((NamedStreamsMeta) opDesc.output).streams.keySet()) {
                        String outp = getProperty(taskConfig, Constants.OP_OUTPUT_PREFIX + taskItem + "." + name);

                        if (outp != null) {
                            opDef.output.named.put(name, outp);

                            task.streams.compute(outp, (k, ds) -> {
                                if (ds == null) {
                                    ds = new TaskDefinitionLanguage.DataStream();
                                }
                                return ds;
                            });
                        }
                    }

                    if (opDef.output.named.isEmpty()) {
                        throw new InvalidConfigValueException("Operation " + taskItem + " produces named output(s), but it hasn't been configured to emit one");
                    }
                }

                tiDef = opDef;
            } else {
                tiDef = TaskDefinitionLanguage.createDirective(task, taskItem);
            }

            task.items.add(tiDef);
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

        dataStreams.forEach(dsName -> task.streams.compute(dsName, (k, ds) -> {
            if (ds == null) {
                ds = new TaskDefinitionLanguage.DataStream();
            }

            ds.input = new TaskDefinitionLanguage.StreamDesc();
            ds.input.columns = getProperty(taskConfig, Constants.DS_INPUT_COLUMNS_PREFIX + dsName);
            ds.input.delimiter = getProperty(taskConfig, Constants.DS_INPUT_DELIMITER_PREFIX + dsName);
            ds.input.partCount = getProperty(taskConfig, Constants.DS_INPUT_PART_COUNT_PREFIX + dsName);

            ds.output = new TaskDefinitionLanguage.StreamDesc();
            ds.output.columns = getProperty(taskConfig, Constants.DS_OUTPUT_COLUMNS_PREFIX + dsName);
            ds.output.delimiter = getProperty(taskConfig, Constants.DS_OUTPUT_DELIMITER_PREFIX + dsName);
            ds.output.partCount = getProperty(taskConfig, Constants.DS_OUTPUT_PART_COUNT_PREFIX + dsName);

            return ds;
        }));
        TaskDefinitionLanguage.DataStream defaultDs = task.streams.get(Constants.DEFAULT_DS);
        if (StringUtils.isEmpty(defaultDs.input.delimiter)) {
            defaultDs.input.delimiter = getProperty(taskConfig, Constants.DS_INPUT_DELIMITER);
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
