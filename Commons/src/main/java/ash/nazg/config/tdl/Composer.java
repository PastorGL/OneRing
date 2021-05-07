/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.config.tdl;

import ash.nazg.spark.Operations;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Composer {
    public static TaskDefinitionLanguage.Task composeTasks(Map<String, TaskDefinitionLanguage.Task> tasks, Map<String, Map<String, String>> dsMergeMap, boolean full) {
        TaskDefinitionLanguage.Task composed = TaskDefinitionLanguage.createTask();
        Set<String> allInput = new HashSet<>();
        Set<String> allOutput = new HashSet<>();
        Collection<String> lastOutputs = Collections.emptyList();

        int i = 0, last = tasks.size() - 1;

        for (Map.Entry<String, TaskDefinitionLanguage.Task> e : tasks.entrySet()) {
            String alias = e.getKey();
            TaskDefinitionLanguage.Task task = e.getValue();
            final Function<String, String> replacer = s -> {
                for (Map.Entry<String, Map<String, String>> entry : dsMergeMap.entrySet()) {
                    String origin = entry.getKey();
                    Map<String, String> replaces = entry.getValue();
                    if (replaces.containsKey(alias) && replaces.get(alias).equals(s)) {
                        return origin;
                    }
                }

                return alias + "_" + s;
            };

            if (task.dataStreams != null) {
                final int _i = i;

                StreamResolver dsResolver = new StreamResolver(task.dataStreams);
                task.dataStreams.forEach((oldName, oldDs) -> {
                    if (oldName.equals(Constants.DEFAULT_DS)) {
                        if (_i == last) {
                            TaskDefinitionLanguage.DataStream defDs = composed.dataStreams.get(Constants.DEFAULT_DS);
                            defDs.input = oldDs.input;
                            defDs.output = oldDs.output;
                        }
                        return;
                    }

                    String name = replacer.apply(oldName);

                    TaskDefinitionLanguage.DataStream ds = new TaskDefinitionLanguage.DataStream();

                    if (oldDs.input != null) {
                        ds.input = new TaskDefinitionLanguage.StreamDesc();
                        ds.input.delimiter = String.valueOf(dsResolver.inputDelimiter(oldName));

                        if (oldDs.input.columns != null) {
                            ds.input.columns = String.join(Constants.COMMA, dsResolver.rawInputColumns(oldName));
                        }

                        if (task.input.contains(oldName)) {
                            ds.input.path = dsResolver.inputPath(oldName);
                        }

                        if (oldDs.input.partCount != null) {
                            ds.input.partCount = String.valueOf(dsResolver.inputParts(oldName));
                        }
                    }
                    if (oldDs.output != null) {
                        ds.output = new TaskDefinitionLanguage.StreamDesc();
                        ds.output.delimiter = String.valueOf(dsResolver.outputDelimiter(oldName));

                        if (oldDs.output.columns != null) {
                            List<String> columns = new ArrayList<>();
                            for (String column : dsResolver.outputColumns(oldName)) {
                                String[] c = column.split("\\.", 2);

                                if (c.length > 1) {
                                    columns.add(replacer.apply(c[0]) + "." + c[1]);
                                } else {
                                    columns.add(column);
                                }
                            }
                            ds.output.columns = String.join(Constants.COMMA, columns);
                        }

                        if (task.output.contains(oldName)) {
                            ds.output.path = dsResolver.outputPath(oldName);
                        }

                        if (oldDs.output.partCount != null) {
                            ds.output.partCount = String.valueOf(dsResolver.outputParts(oldName));
                        }
                    }

                    TaskDefinitionLanguage.DataStream existing = composed.dataStreams.get(name);

                    if (existing != null) {
                        if ((existing.input == null) && (ds.input != null)) {
                            existing.input = ds.input;
                        }
                        if ((existing.output == null) && (ds.output != null)) {
                            existing.output = ds.output;
                        }
                    } else {
                        composed.dataStreams.put(name, ds);
                    }
                });
            }

            if (task.taskItems != null) {
                for (TaskDefinitionLanguage.TaskItem ti : task.taskItems) {
                    if (ti instanceof TaskDefinitionLanguage.Operation) {
                        TaskDefinitionLanguage.Operation oldOp = (TaskDefinitionLanguage.Operation) ti;
                        OperationResolver opResolver = new OperationResolver(Operations.availableOperations.get(oldOp.verb).description, oldOp);

                        TaskDefinitionLanguage.Operation op = TaskDefinitionLanguage.createOperation(composed);
                        op.name = alias + "_" + oldOp.name;

                        if (oldOp.definitions != null) {
                            op.definitions = TaskDefinitionLanguage.createDefinitions(composed);

                            oldOp.definitions.keySet().forEach(name -> {
                                String value = opResolver.definition(name);

                                if (name.endsWith(Constants.COLUMN_SUFFIX)) {
                                    String[] c = value.split("\\.", 2);

                                    value = replacer.apply(c[0]) + "." + c[1];
                                }
                                if (name.endsWith(Constants.COLUMNS_SUFFIX)) {
                                    String[] cols = value.split(Constants.COMMA);
                                    String[] repl = new String[cols.length];
                                    for (int j = 0; j < cols.length; j++) {
                                        String col = cols[j];
                                        String[] c = col.split("\\.", 2);
                                        repl[j] = replacer.apply(c[0]) + "." + c[1];
                                    }
                                    value = String.join(Constants.COMMA, repl);
                                }

                                op.definitions.put(name, value);
                            });
                        }

                        if (oldOp.inputs.positionalNames != null) {
                            op.inputs.positionalNames = Arrays.stream(opResolver.positionalInputs())
                                    .map(replacer)
                                    .collect(Collectors.joining(Constants.COMMA));
                        }
                        if (oldOp.inputs.named != null) {
                            oldOp.inputs.named.keySet()
                                    .forEach(in -> op.inputs.named.put(in, replacer.apply(opResolver.namedInput(in))));
                        }
                        if (oldOp.outputs.positionalNames != null) {
                            op.outputs.positionalNames = Arrays.stream(opResolver.positionalOutputs())
                                    .map(replacer)
                                    .collect(Collectors.joining(Constants.COMMA));
                        }
                        if (oldOp.outputs.named != null) {
                            oldOp.outputs.named.keySet()
                                    .forEach(in -> op.outputs.named.put(in, replacer.apply(oldOp.outputs.named.get(in))));
                        }

                        composed.taskItems.add(oldOp);
                    } else {
                        composed.taskItems.add(ti);
                    }
                }

                if (task.input != null) {
                    allInput.addAll(task.input.stream()
                            .map(replacer)
                            .collect(Collectors.toList())
                    );
                }

                if (task.output != null) {
                    lastOutputs = task.output.stream()
                            .map(replacer)
                            .collect(Collectors.toList());

                    allOutput.addAll(lastOutputs);
                }

                for (String layer : task.foreignLayers()) {
                    composed.foreignLayer(layer, task.foreignLayer(layer));
                }
            }

            i++;
        }

        composed.input = allInput.stream().filter(s -> !allOutput.contains(s)).collect(Collectors.toList());
        composed.output = new ArrayList<>(full ? lastOutputs : allOutput);

        return composed;
    }

    public static Map<String, Map<String, String>> parseMergeMap(List<String> source) {
        final Map<String, Map<String, String>> mergeMap = new HashMap<>();
        source.forEach(l -> {
            String[] m = l.split("\\s+");

            String origin = null;

            HashMap<String, String> replaces = new HashMap<>();
            for (String r : m) {
                String[] repl = r.split("[:.]", 2);

                replaces.put(repl[0], repl[1]);

                if (origin == null) {
                    origin = repl[1];
                }
            }

            mergeMap.put(origin, replaces);
        });

        return mergeMap;
    }
}
