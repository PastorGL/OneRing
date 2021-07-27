/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.config.tdl;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.Writer;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class PropertiesWriter {
    public static void writeProperties(TaskDefinitionLanguage.Task task, Writer writer) throws IOException {
        Map<String, String> properties = new HashMap<>();

        List<String> opNames = new ArrayList<>();
        for (TaskDefinitionLanguage.TaskItem ti : task.items) {
            if (ti instanceof TaskDefinitionLanguage.Operation) {
                TaskDefinitionLanguage.Operation op = (TaskDefinitionLanguage.Operation) ti;

                String opName = op.name;
                opNames.add(opName);

                properties.put(Constants.OP_OPERATION_PREFIX + opName, op.verb);

                if (op.definitions != null) {
                    for (Map.Entry<String, String> entry : op.definitions.entrySet()) {
                        String value = entry.getValue();
                        if (!StringUtils.isEmpty(value)) {
                            properties.put(Constants.OP_DEFINITION_PREFIX + opName + "." + entry.getKey(), value);
                        }
                    }
                }

                if (!StringUtils.isEmpty(op.input.positional)) {
                    properties.put(Constants.OP_INPUTS_PREFIX + opName, op.input.positional);
                } else if (op.input.named != null) {
                    for (Map.Entry<String, String> entry : op.input.named.entrySet()) {
                        String value = entry.getValue();
                        if (!StringUtils.isEmpty(value)) {
                            properties.put(Constants.OP_INPUT_PREFIX + opName + "." + entry.getKey(), value);
                        }
                    }
                }

                if (!StringUtils.isEmpty(op.output.positional)) {
                    properties.put(Constants.OP_OUTPUTS_PREFIX + opName, op.output.positional);
                } else if (op.output.named != null) {
                    for (Map.Entry<String, String> entry : op.output.named.entrySet()) {
                        String value = entry.getValue();
                        if (!StringUtils.isEmpty(value)) {
                            properties.put(Constants.OP_OUTPUT_PREFIX + opName + "." + entry.getKey(), value);
                        }
                    }
                }
            } else if (ti instanceof TaskDefinitionLanguage.Directive) {
                TaskDefinitionLanguage.Directive dir = (TaskDefinitionLanguage.Directive) ti;

                opNames.add(dir.directive);
            }
        }

        for (Map.Entry<String, TaskDefinitionLanguage.DataStream> dss : task.streams.entrySet()) {
            String dsName = dss.getKey();
            TaskDefinitionLanguage.DataStream ds = dss.getValue();

            if (dsName.equals(Constants.DEFAULT_DS)) {
                if (ds.output != null) {
                    if (!StringUtils.isEmpty(ds.output.delimiter) && (ds.output.delimiter.charAt(0) != Constants.DEFAULT_DELIMITER)) {
                        properties.put(Constants.DS_OUTPUT_DELIMITER, ds.output.delimiter);
                    }
                }
                if (ds.input != null) {
                    if (!StringUtils.isEmpty(ds.input.delimiter) && (ds.input.delimiter.charAt(0) != Constants.DEFAULT_DELIMITER)) {
                        properties.put(Constants.DS_INPUT_DELIMITER, ds.input.delimiter);
                    }
                }

                continue;
            }

            if (ds.input != null) {
                if (!StringUtils.isEmpty(ds.input.partCount)) {
                    properties.put(Constants.DS_INPUT_PART_COUNT_PREFIX + dsName, ds.input.partCount);
                }
                if (!StringUtils.isEmpty(ds.input.columns)) {
                    properties.put(Constants.DS_INPUT_COLUMNS_PREFIX + dsName, ds.input.columns);
                }
                if (!StringUtils.isEmpty(ds.input.delimiter) && (ds.input.delimiter.charAt(0) != Constants.DEFAULT_DELIMITER)) {
                    properties.put(Constants.DS_INPUT_DELIMITER_PREFIX + dsName, ds.input.delimiter);
                }
            }

            if (ds.output != null) {
                if (!StringUtils.isEmpty(ds.output.partCount)) {
                    properties.put(Constants.DS_OUTPUT_PART_COUNT_PREFIX + dsName, ds.output.partCount);
                }
                if (!StringUtils.isEmpty(ds.output.columns)) {
                    properties.put(Constants.DS_OUTPUT_COLUMNS_PREFIX + dsName, ds.output.columns);
                }
                if (!StringUtils.isEmpty(ds.output.delimiter) && (ds.output.delimiter.charAt(0) != Constants.DEFAULT_DELIMITER)) {
                    properties.put(Constants.DS_OUTPUT_DELIMITER_PREFIX + dsName, ds.output.delimiter);
                }
            }
        }

        properties.put(Constants.TASK_OPERATIONS, String.join(Constants.COMMA, opNames));
        if (task.input != null) {
            properties.put(Constants.TASK_INPUT, String.join(Constants.COMMA, task.input));
        }
        if (task.output != null) {
            properties.put(Constants.TASK_OUTPUT, String.join(Constants.COMMA, task.output));
        }

        for (String l : task.foreignLayers()) {
            for (Map.Entry<String, String> entry : task.foreignLayer(l).entrySet()) {
                properties.put(l + "." + entry.getKey(), entry.getValue());
            }
        }

        DocComparator cmp = new DocComparator(opNames);

        Map<String, String> sorted = properties.keySet().stream()
                .sorted(cmp)
                .collect(Collectors.toMap(k -> k, properties::get, (o, n) -> n, LinkedHashMap::new));

        String prefix = StringUtils.isEmpty(task.prefix) ? "" : task.prefix + ".";
        String kk = null;
        for (Map.Entry<String, String> e : sorted.entrySet()) {
            String k = e.getKey();

            if (kk != null) {
                int difference = cmp.compare(k, kk);
                if (difference >= 2) {
                    writer.write("\n\n");
                } else {
                    writer.write("\n");
                }
            }
            writer.write(prefix + k + "=" + String.join("\\", replNL.split(e.getValue())));
            kk = k;
        }
        writer.write("\n");
    }

    private static final Pattern replNL = Pattern.compile("$", Pattern.MULTILINE);

    private static class DocComparator implements Comparator<Object> {
        private final Map<String, Integer> priorities;

        public DocComparator(List<String> opOrder) {
            priorities = new HashMap() {
                private int p = 0;

                {
                    put(Constants.DIST_LAYER);
                    put(Constants.INPUT_LAYER);
                    put(Constants.TASK_INPUT);
                    put(Constants.METRICS_LAYER);
                    put(Constants.TASK_OPERATIONS);
                    put(Constants.DS_INPUT_PREFIX);
                    for (String op : opOrder) {
                        put(Constants.OP_OPERATION_PREFIX + op);
                        put1(Constants.OP_INPUTS_PREFIX + op);
                        put0(Constants.OP_INPUT_PREFIX + op);
                        put1(Constants.OP_DEFINITION_PREFIX + op);
                        put1(Constants.OP_OUTPUT_PREFIX + op);
                        put0(Constants.OP_OUTPUTS_PREFIX + op);
                    }
                    put(Constants.DS_OUTPUT_PREFIX);
                    put(Constants.TASK_OUTPUT);
                    put(Constants.OUTPUT_LAYER);

                    ++p;
                }

                private void put(String prefix) {
                    put(prefix, p += 2);
                }

                private void put1(String prefix) {
                    put(prefix, ++p);
                }

                private void put0(String prefix) {
                    put(prefix, p);
                }

                public Integer get(Object k) {
                    for (Object e : entrySet()) {
                        if (((String) k).startsWith(((Entry<String, Integer>) e).getKey())) {
                            return ((Entry<String, Integer>) e).getValue();
                        }
                    }

                    return ++p;
                }
            };
        }

        @Override
        public int compare(Object o1, Object o2) {
            return priorities.get(o1) - priorities.get(o2);
        }
    }
}
