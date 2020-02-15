/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.config.tdl;

import ash.nazg.config.*;
import ash.nazg.spark.Operation;
import ash.nazg.spark.Operations;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.*;
import java.util.function.Function;

import static ash.nazg.config.WrapperConfig.*;

public class PropertiesConverter {
    public static final String DEFAULT_DS = "_default";

    static public TaskDefinitionLanguage.Task toTask(String prefix, Properties properties) throws Exception {
        WrapperConfig taskConfig = new WrapperConfig();
        taskConfig.setPrefix(prefix);
        taskConfig.setProperties(properties);

        return toTask(taskConfig);
    }

    static public TaskDefinitionLanguage.Task toTask(WrapperConfig taskConfig) throws Exception {
        List<TaskDefinitionLanguage.Operation> opDefs = new ArrayList<>();
        Map<String, TaskDefinitionLanguage.DataStream> dsDefs = new HashMap<>();

        Set<String> sink = taskConfig.getInputSink();
        Set<String> tees = taskConfig.getTeeOutput();

        Map<String, Boolean> dsColBased = new HashMap<>();

        Map<String, Operation.Info> ao = Operations.getAvailableOperations();

        Properties allOpProperties = taskConfig.getProperties();
        for (Map.Entry<String, String> operation : taskConfig.getOperations().entrySet()) {
            String verb = operation.getValue();
            String name = operation.getKey();

            if (!ao.containsKey(verb)) {
                throw new InvalidConfigValueException("Operation '" + name + "' has unknown verb '" + verb + "'");
            }

            Operation chainedOp = ao.get(verb).operationClass.newInstance();

            TaskDescriptionLanguage.Operation opDesc = chainedOp.description();

            TaskDefinitionLanguage.Operation opDef = new TaskDefinitionLanguage.Operation();

            opDef.name = name;
            opDef.verb = verb;

            OperationConfig opConf = new OperationConfig(allOpProperties, opDesc, name);

            if (opDesc.definitions != null) {
                List<TaskDefinitionLanguage.Definition> defs = new ArrayList<>();
                List<TaskDefinitionLanguage.DynamicDef> dynDefs = new ArrayList<>();
                for (int i = 0; i < opDesc.definitions.length; i++) {
                    TaskDescriptionLanguage.DefBase defDesc = opDesc.definitions[i];

                    if (defDesc instanceof TaskDescriptionLanguage.DynamicDef) {
                        TaskDescriptionLanguage.DynamicDef dynDef = (TaskDescriptionLanguage.DynamicDef) defDesc;
                        for (Map.Entry<String, Object> e : opConf.defs.entrySet()) {
                            String dynDefName = e.getKey();
                            if (dynDefName.startsWith(dynDef.prefix)) {

                                TaskDefinitionLanguage.DynamicDef def = new TaskDefinitionLanguage.DynamicDef();
                                def.name = dynDefName;
                                def.value = stringify(e.getValue());

                                dynDefs.add(def);
                            }
                        }
                    } else {
                        TaskDescriptionLanguage.Definition defDe = (TaskDescriptionLanguage.Definition) defDesc;

                        TaskDefinitionLanguage.Definition def = new TaskDefinitionLanguage.Definition();
                        def.name = defDe.name;

                        String value = opConf.defs.get(defDe.name) == null ? null : stringify(opConf.defs.get(defDe.name));

                        if (defDe.optional && Objects.equals(defDe.defaults, value)) {
                            def.useDefaults = true;
                        } else {
                            def.value = value;
                        }

                        defs.add(def);
                    }
                }
                opDef.definitions = defs.toArray(new TaskDefinitionLanguage.Definition[0]);
                opDef.dynamicDefs = dynDefs.toArray(new TaskDefinitionLanguage.DynamicDef[0]);
            }

            opDefs.add(opDef);

            Map<String, TaskDefinitionLanguage.DataStream> opDsDefs = new HashMap<>();

            opDef.inputs = new TaskDefinitionLanguage.OpStreams();
            if (opDesc.inputs.positional != null) {
                List<String> posInputs = opConf.inputs;
                opDef.inputs.positionalNames = posInputs.toArray(new String[0]);
                for (String inp : posInputs) {
                    dsColBased.compute(inp, (inpName, cb) -> (cb == null)
                            ? opDesc.inputs.positional.columnBased
                            : cb || opDesc.inputs.positional.columnBased
                    );

                    opDsDefs.put(inp, dsDefs.compute(inp, (inpName, ds) -> {
                        if (ds == null) {
                            ds = new TaskDefinitionLanguage.DataStream();
                            ds.name = inpName;
                        }
                        if (ds.input == null) {
                            ds.input = new TaskDefinitionLanguage.StreamDesc();
                        }

                        return ds;
                    }));
                }
            } else if (opDesc.inputs.named != null) {
                opDef.inputs.named = new TaskDefinitionLanguage.NamedStream[opDesc.inputs.named.length];
                for (int i = 0; i < opDesc.inputs.named.length; i++) {
                    TaskDescriptionLanguage.NamedStream inDesc = opDesc.inputs.named[i];
                    TaskDefinitionLanguage.NamedStream input = new TaskDefinitionLanguage.NamedStream();
                    opDef.inputs.named[i] = input;
                    input.name = inDesc.name;
                    input.value = opConf.namedInputs.get(inDesc.name);
                    dsColBased.compute(input.value, (inpName, cb) -> (cb == null)
                            ? inDesc.columnBased
                            : cb || inDesc.columnBased
                    );

                    opDsDefs.put(input.value, dsDefs.compute(input.value, (inpName, ds) -> {
                        if (ds == null) {
                            ds = new TaskDefinitionLanguage.DataStream();
                            ds.name = inpName;
                        }
                        if (ds.input == null) {
                            ds.input = new TaskDefinitionLanguage.StreamDesc();
                        }

                        return ds;
                    }));
                }
            }

            opDef.outputs = new TaskDefinitionLanguage.OpStreams();
            if (opDesc.outputs.positional != null) {
                List<String> posOutputs = opConf.outputs;
                opDef.outputs.positionalNames = posOutputs.toArray(new String[0]);
                for (String outp : posOutputs) {
                    dsColBased.compute(outp, (outName, cb) -> (cb == null)
                            ? opDesc.outputs.positional.columnBased
                            : cb || opDesc.outputs.positional.columnBased
                    );

                    opDsDefs.put(outp, dsDefs.compute(outp, (outpName, ds) -> {
                        if (ds == null) {
                            ds = new TaskDefinitionLanguage.DataStream();
                            ds.name = outpName;
                        }
                        if (ds.output == null) {
                            ds.output = new TaskDefinitionLanguage.StreamDesc();
                        }

                        return ds;
                    }));
                }
            } else if (opDesc.outputs.named != null) {
                List<TaskDefinitionLanguage.NamedStream> nOuts = new ArrayList<>();
                for (int i = 0; i < opDesc.outputs.named.length; i++) {
                    TaskDescriptionLanguage.NamedStream outDesc = opDesc.outputs.named[i];
                    String nOutName = opConf.namedOutputs.get(outDesc.name);

                    if (nOutName != null) {
                        TaskDefinitionLanguage.NamedStream output = new TaskDefinitionLanguage.NamedStream();
                        output.name = outDesc.name;
                        output.value = nOutName;
                        dsColBased.compute(nOutName, (outName, cb) -> (cb == null)
                                ? outDesc.columnBased
                                : cb || outDesc.columnBased
                        );
                        nOuts.add(output);

                        opDsDefs.put(nOutName, dsDefs.compute(nOutName, (outpName, ds) -> {
                            if (ds == null) {
                                ds = new TaskDefinitionLanguage.DataStream();
                                ds.name = outpName;
                            }
                            if (ds.output == null) {
                                ds.output = new TaskDefinitionLanguage.StreamDesc();
                            }

                            return ds;
                        }));
                    }
                }
                if (!nOuts.isEmpty()) {
                    opDef.outputs.named = nOuts.toArray(new TaskDefinitionLanguage.NamedStream[0]);
                }
            }

            DataStreamsConfig dsc = opConf.dsc;

            Set<String> dss = new HashSet<>(opConf.namedInputs.values());
            dss.addAll(opConf.inputs);

            TaskDefinitionLanguage.DataStream dsDef;
            for (String ds : dss) {
                dsDef = opDsDefs.get(ds);

                TaskDefinitionLanguage.StreamDesc streamDesc = dsDef.input;
                if (dsColBased.get(ds)) {
                    streamDesc.columns = dsc.inputColumnsRaw.get(ds);
                    char delimiter = dsc.inputDelimiter(ds);
                    streamDesc.delimiter = (delimiter != PropertiesConfig.DEFAULT_DELIMITER) ? String.valueOf(delimiter) : null;
                }
                String pc = taskConfig.getDsInputPartCount(ds);
                streamDesc.partCount = "-1".equals(pc) ? null : pc;
                streamDesc.path = taskConfig.inputPath(ds);
            }

            dss = new HashSet<>(opConf.namedOutputs.values());
            dss.addAll(opConf.outputs);

            for (String ds : dss) {
                dsDef = opDsDefs.get(ds);

                if (dsDef != null) {
                    TaskDefinitionLanguage.StreamDesc streamDesc = dsDef.output;
                    if (dsColBased.get(ds)) {
                        streamDesc.columns = dsc.outputColumns.get(ds);
                        char delimiter = dsc.outputDelimiter(ds);
                        streamDesc.delimiter = (delimiter != PropertiesConfig.DEFAULT_DELIMITER) ? String.valueOf(delimiter) : null;
                    }
                    String pc = taskConfig.getDsOutputPartCount(ds);
                    streamDesc.partCount = "-1".equals(pc) ? null : pc;
                    if (tees.contains(ds)) {
                        streamDesc.path = taskConfig.outputPath(ds);
                    }
                }
            }

            dsDefs.putAll(opDsDefs);
        }

        DataStreamsConfig dsc = new DataStreamsConfig(allOpProperties, null, null, tees, null, null);

        TaskDefinitionLanguage.DataStream defaultDs = new TaskDefinitionLanguage.DataStream();
        defaultDs.name = DEFAULT_DS;
        defaultDs.output = new TaskDefinitionLanguage.StreamDesc();
        defaultDs.output.path = taskConfig.defaultOutputPath();
        defaultDs.output.delimiter = (dsc.defaultOutputDelimiter() != PropertiesConfig.DEFAULT_DELIMITER) ? String.valueOf(dsc.defaultOutputDelimiter()) : null;
        defaultDs.input = new TaskDefinitionLanguage.StreamDesc();
        defaultDs.input.delimiter = (dsc.defaultInputDelimiter() != PropertiesConfig.DEFAULT_DELIMITER) ? String.valueOf(dsc.defaultInputDelimiter()) : null;

        dsDefs.put(DEFAULT_DS, defaultDs);

        TaskDefinitionLanguage.Task task = new TaskDefinitionLanguage.Task();
        task.operations = opDefs.toArray(new TaskDefinitionLanguage.Operation[0]);
        task.dataStreams = dsDefs.values().toArray(new TaskDefinitionLanguage.DataStream[0]);
        task.sink = sink.toArray(new String[0]);
        task.tees = tees.toArray(new String[0]);
        task.prefix = taskConfig.getPrefix();

        task.distcp = taskConfig.getLayerProperties(DISTCP_PREFIX);
        task.input = taskConfig.getLayerProperties(INPUT_PREFIX);
        task.output = taskConfig.getLayerProperties(OUTPUT_PREFIX);

        return task;
    }

    static private String stringify(Object value) {
        return value.getClass() == String[].class
                ? String.join(",", (String[]) value)
                : String.valueOf(value);
    }

    static public String toJSON(String prefix, Properties properties) throws Exception {
        ObjectMapper om = new ObjectMapper();

        return om.writeValueAsString(toTask(prefix, properties));
    }

    static public Properties toProperties(String json) throws Exception {
        ObjectMapper om = new ObjectMapper();
        TaskDefinitionLanguage.Task task = om.readValue(json, TaskDefinitionLanguage.Task.class);

        return toProperties(task);
    }

    static public Properties toProperties(TaskDefinitionLanguage.Task task) {
        Properties properties = new Properties();
        List<String> opNames = new ArrayList<>();
        for (TaskDefinitionLanguage.Operation op : task.operations) {
            String opName = op.name;
            opNames.add(opName);

            properties.put(OP_OPERATION_PREFIX + opName, op.verb);

            if (op.definitions != null) {
                for (TaskDefinitionLanguage.Definition def : op.definitions) {
                    if ((def.useDefaults == null) || !def.useDefaults) {
                        properties.put(OperationConfig.OP_DEFINITION_PREFIX + opName + "." + def.name, stringify(def.value));
                    }
                }
            }

            if (op.dynamicDefs != null) {
                for (TaskDefinitionLanguage.DynamicDef def : op.dynamicDefs) {
                    properties.put(OperationConfig.OP_DEFINITION_PREFIX + opName + "." + def.name, stringify(def.value));
                }
            }

            if (op.inputs.positionalNames != null) {
                properties.put(OperationConfig.OP_INPUTS_PREFIX + opName, String.join(PropertiesConfig.COMMA, op.inputs.positionalNames));
            } else if (op.inputs.named != null) {
                for (TaskDefinitionLanguage.NamedStream in : op.inputs.named) {
                    properties.put(OperationConfig.OP_INPUT_PREFIX + opName + "." + in.name, in.value);
                }
            }

            if (op.outputs.positionalNames != null) {
                properties.put(OperationConfig.OP_OUTPUTS_PREFIX + opName, String.join(PropertiesConfig.COMMA, op.outputs.positionalNames));
            } else if (op.outputs.named != null) {
                for (TaskDefinitionLanguage.NamedStream out : op.outputs.named) {
                    properties.put(OperationConfig.OP_OUTPUT_PREFIX + opName + "." + out.name, out.value);
                }
            }
        }

        for (TaskDefinitionLanguage.DataStream ds : task.dataStreams) {
            String dsName = ds.name;

            if (dsName.equals(DEFAULT_DS)) {
                if (ds.output != null) {
                    if (ds.output.path != null) {
                        properties.put(DS_OUTPUT_PATH, ds.output.path);
                    }
                    if ((ds.output.delimiter != null) && (ds.output.delimiter.charAt(0) != PropertiesConfig.DEFAULT_DELIMITER)) {
                        properties.put(DataStreamsConfig.DS_OUTPUT_DELIMITER, ds.output.delimiter);
                    }
                }
                if (ds.input != null) {
                    if ((ds.input.delimiter != null) && (ds.input.delimiter.charAt(0) != PropertiesConfig.DEFAULT_DELIMITER)) {
                        properties.put(DataStreamsConfig.DS_INPUT_DELIMITER, ds.input.delimiter);
                    }
                }

                continue;
            }

            if (ds.input != null) {
                if (ds.input.path != null) {
                    properties.put(DS_INPUT_PATH_PREFIX + dsName, ds.input.path);
                }
                if (ds.input.partCount != null) {
                    properties.put(DS_INPUT_PART_COUNT_PREFIX + dsName, ds.input.partCount);
                }
                if (ds.input.columns != null) {
                    properties.put(DataStreamsConfig.DS_INPUT_COLUMNS_PREFIX + dsName, String.join(PropertiesConfig.COMMA, ds.input.columns));
                }
                if ((ds.input.delimiter != null) && (ds.input.delimiter.charAt(0) != PropertiesConfig.DEFAULT_DELIMITER)) {
                    properties.put(DataStreamsConfig.DS_INPUT_DELIMITER_PREFIX + dsName, ds.input.delimiter);
                }
            }

            if (ds.output != null) {
                if (ds.output.path != null) {
                    properties.put(DS_OUTPUT_PATH_PREFIX + dsName, ds.output.path);
                }
                if (ds.output.partCount != null) {
                    properties.put(DS_OUTPUT_PART_COUNT_PREFIX + dsName, ds.output.partCount);
                }
                if (ds.output.columns != null) {
                    properties.put(DataStreamsConfig.DS_OUTPUT_COLUMNS_PREFIX + dsName, String.join(PropertiesConfig.COMMA, ds.output.columns));
                }
                if ((ds.output.delimiter != null) && (ds.output.delimiter.charAt(0) != PropertiesConfig.DEFAULT_DELIMITER)) {
                    properties.put(DataStreamsConfig.DS_OUTPUT_DELIMITER_PREFIX + dsName, ds.output.delimiter);
                }
            }
        }

        properties.put(TASK_OPERATIONS, String.join(PropertiesConfig.COMMA, opNames));
        properties.put(TASK_INPUT_SINK, String.join(PropertiesConfig.COMMA, task.sink));
        properties.put(TASK_TEE_OUTPUT, String.join(PropertiesConfig.COMMA, task.tees));

        if (task.distcp != null) {
            task.distcp.forEach((k, v) -> properties.put(DISTCP_PREFIX + k, v));
        }
        if (task.input != null) {
            task.input.forEach((k, v) -> properties.put(INPUT_PREFIX + k, v));
        }
        if (task.output != null) {
            task.output.forEach((k, v) -> properties.put(OUTPUT_PREFIX + k, v));
        }

        if (task.prefix != null) {
            Properties prefixed = new Properties();
            properties.forEach((k, v) -> prefixed.put(task.prefix + "." + k, v));
            return prefixed;
        }

        return properties;
    }

    public static TaskDefinitionLanguage.Task composeTasks(Map<String, TaskDefinitionLanguage.Task> tasks, Map<String, Map<String, String>> dsMergeMap, boolean full) {
        TaskDefinitionLanguage.Task composed = new TaskDefinitionLanguage.Task();
        Set<String> allTees = new HashSet<>();
        List<TaskDefinitionLanguage.Operation> operations = new ArrayList<>();
        Map<String, TaskDefinitionLanguage.DataStream> dataStreams = new HashMap<>();

        TaskDefinitionLanguage.Task lastTask = null;
        int i = 0, last = tasks.size() - 1;

        composed.distcp = new Properties();
        composed.input = new Properties();
        composed.output = new Properties();

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

            if (task.distcp != null) {
                composed.distcp.putAll(task.distcp);
            }
            if (task.input != null) {
                composed.input.putAll(task.input);
            }
            if (task.output != null) {
                composed.output.putAll(task.output);
            }

            if (task.sink != null) {
                task.sink = Arrays.stream(task.sink)
                        .map(replacer)
                        .toArray(String[]::new);
            }

            if (task.tees != null) {
                task.tees = Arrays.stream(task.tees)
                        .map(replacer)
                        .toArray(String[]::new);

                allTees.addAll(Arrays.asList(task.tees));
            }

            if (task.dataStreams != null) {
                final int _i = i;
                final TaskDefinitionLanguage.DataStream _default = Arrays.stream(task.dataStreams)
                        .filter(ds -> ds.name.equals(DEFAULT_DS))
                        .peek(ds -> {
                            if (_i == last) {
                                dataStreams.put(DEFAULT_DS, ds);
                            }
                        })
                        .findFirst().get();

                task.dataStreams = Arrays.stream(task.dataStreams)
                        .filter(ds -> !ds.name.equals(DEFAULT_DS))
                        .peek(ds -> {
                            ds.name = replacer.apply(ds.name);

                            if (ds.input != null) {
                                ds.input.delimiter = (ds.input.delimiter != null) ? ds.input.delimiter : _default.input.delimiter;
                            }
                            if (ds.output != null) {
                                ds.output.delimiter = (ds.output.delimiter != null) ? ds.output.delimiter : _default.output.delimiter;

                                if (ds.output.columns != null) {
                                    List<String> columns = new ArrayList<>();
                                    for (String column : ds.output.columns) {
                                        String[] c = column.split("\\.", 2);

                                        if (c.length > 1) {
                                            columns.add(replacer.apply(c[0]) + "." + c[1]);
                                        } else {
                                            columns.add(column);
                                        }
                                    }
                                    ds.output.columns = columns.toArray(new String[0]);
                                }

                                if (Arrays.asList(task.tees).contains(ds.name) && (_default.output.path != null)) {
                                    ds.output.path = (ds.output.path != null) ? ds.output.path : _default.output.path + "/" + ds.name;
                                }
                            }

                            TaskDefinitionLanguage.DataStream existing = dataStreams.get(ds.name);

                            if (existing != null) {
                                if ((existing.input == null) && (ds.input != null)) {
                                    existing.input = ds.input;
                                }
                                if ((existing.output == null) && (ds.output != null)) {
                                    existing.output = ds.output;
                                }
                            } else {
                                dataStreams.put(ds.name, ds);
                            }
                        })
                        .toArray(TaskDefinitionLanguage.DataStream[]::new);
            }

            if (task.operations != null) {
                task.operations = Arrays.stream(task.operations)
                        .peek(op -> {
                            op.name = alias + "_" + op.name;

                            if (op.definitions != null) {
                                op.definitions = Arrays.stream(op.definitions)
                                        .peek(def -> {
                                            if ((def.useDefaults == null) || !def.useDefaults) {
                                                if (def.name.endsWith(OperationConfig.COLUMN_SUFFIX)) {
                                                    String[] c = def.value.split("\\.", 2);

                                                    def.value = replacer.apply(c[0]) + "." + c[1];
                                                }
                                                if (def.name.endsWith(OperationConfig.COLUMNS_SUFFIX)) {
                                                    String[] cols = def.value.split(COMMA);
                                                    String[] repl = new String[cols.length];
                                                    for (int j = 0; j < cols.length; j++) {
                                                        String col = cols[j];
                                                        String[] c = col.split("\\.", 2);
                                                        repl[j] = replacer.apply(c[0]) + "." + c[1];
                                                    }
                                                    def.value = String.join(COMMA, repl);
                                                }
                                            }
                                        })
                                        .toArray(TaskDefinitionLanguage.Definition[]::new);
                            }

                            if (op.inputs.positionalNames != null) {
                                op.inputs.positionalNames = Arrays.stream(op.inputs.positionalNames)
                                        .map(replacer)
                                        .toArray(String[]::new);
                            }
                            if (op.inputs.named != null) {
                                op.inputs.named = Arrays.stream(op.inputs.named)
                                        .peek(in -> in.value = replacer.apply(in.value))
                                        .toArray(TaskDefinitionLanguage.NamedStream[]::new);
                            }
                            if (op.outputs.positionalNames != null) {
                                op.outputs.positionalNames = Arrays.stream(op.outputs.positionalNames)
                                        .map(replacer)
                                        .toArray(String[]::new);
                            }
                            if (op.outputs.named != null) {
                                op.outputs.named = Arrays.stream(op.outputs.named)
                                        .peek(out -> out.value = replacer.apply(out.value))
                                        .toArray(TaskDefinitionLanguage.NamedStream[]::new);
                            }
                        })
                        .toArray(TaskDefinitionLanguage.Operation[]::new);

                operations.addAll(Arrays.asList(task.operations));
            }

            if (i == last) {
                lastTask = task;
            }
            i++;
        }

        Set<String> sink = new HashSet<>();
        for (TaskDefinitionLanguage.Task task : tasks.values()) {
            for (String s : task.sink) {
                if (!allTees.contains(s)) {
                    sink.add(s);
                }
            }
        }

        Set<String> tees = full ? new HashSet<>(Arrays.asList(lastTask.tees)) : allTees;

        composed.sink = sink.toArray(new String[0]);
        composed.tees = tees.toArray(new String[0]);
        composed.operations = operations.toArray(new TaskDefinitionLanguage.Operation[0]);
        composed.dataStreams = dataStreams.values().toArray(new TaskDefinitionLanguage.DataStream[0]);

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
