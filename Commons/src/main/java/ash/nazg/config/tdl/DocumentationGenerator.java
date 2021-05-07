/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.config.tdl;

import ash.nazg.commons.operations.MapToPairOperation;
import ash.nazg.config.Packages;
import ash.nazg.spark.OpInfo;
import ash.nazg.spark.Operation;
import ash.nazg.spark.Operations;
import ash.nazg.spatial.config.ConfigurationParameters;
import ash.nazg.spatial.operations.*;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;

public class DocumentationGenerator {
    public static TaskDefinitionLanguage.Task createExampleTask(OpInfo opInfo, TaskDocumentationLanguage.Operation opDoc, String prefix) {
        List<String> input = new ArrayList<>();
        List<String> output = new ArrayList<>();

        TaskDefinitionLanguage.Task task = TaskDefinitionLanguage.createTask();
        TaskDefinitionLanguage.Operation opDef = TaskDefinitionLanguage.createOperation(task);

        String verb = opInfo.verb;
        TaskDescriptionLanguage.Operation descr = opInfo.description;

        opDef.verb = verb;
        opDef.name = verb;

        Map<String, List<String>> namedColumns = new HashMap<>();
        namedColumns.put(null, new ArrayList<>());

        if (descr.definitions != null) {
            TaskDefinitionLanguage.Definitions defs = TaskDefinitionLanguage.createDefinitions(task);

            Map<String, String> manDescr = parameterListToMap(opDoc.getMandatoryParameters());
            Map<String, String> optDescr = parameterListToMap(opDoc.getOptionalParameters());
            Map<String, String> dynDescr = parameterListToMap(opDoc.getDynamicParameters());

            for (TaskDescriptionLanguage.DefBase db : descr.definitions.values()) {
                if (db instanceof TaskDescriptionLanguage.Definition) {
                    TaskDescriptionLanguage.Definition def = (TaskDescriptionLanguage.Definition) db;

                    String value;

                    if (def.optional) {
                        value = "<* " + def.defaults + ": " + optDescr.get(def.name) + " *>";
                    } else {
                        if (def.clazz.isEnum()) {
                            value = Arrays.stream(def.enumValues).collect(Collectors.joining(", ", "<* One of: ", " *>"));
                        } else {
                            value = "<* " + def.clazz.getSimpleName() + ": " + manDescr.get(def.name) + " *>";
                        }
                    }

                    if (def.name.endsWith(Constants.COLUMN_SUFFIX)) {
                        String[] col = def.name.split("\\.", 3);

                        if (col.length == 3) {
                            namedColumns.computeIfAbsent(col[0], x -> new ArrayList<>());
                            namedColumns.get(col[0]).add(col[1]);

                            value = verb + "_" + col[0] + "." + col[1];
                        } else if (col.length == 2) {
                            namedColumns.get(null).add(col[0]);

                            value = verb + "_0." + col[0];
                        }
                    }

                    defs.put(def.name, value);
                } else {
                    TaskDescriptionLanguage.DynamicDef dyn = (TaskDescriptionLanguage.DynamicDef) db;
                    String value;

                    if (dyn.clazz.isEnum()) {
                        value = Arrays.stream(dyn.enumValues).collect(Collectors.joining(" ", "<* One of: ", " *>"));
                    } else {
                        value = "<* " + dyn.clazz.getSimpleName() + ": " + dynDescr.get(dyn.name) + " *>";
                    }

                    defs.put(dyn.name + "*", value);
                }
            }

            opDef.definitions = defs;
        }

        AtomicInteger dsNum = new AtomicInteger();

        if (descr.inputs.positional != null) {
            StreamType st = descr.inputs.positional.types[0];

            List<String> posInputs = new ArrayList<>();
            int cnt = (descr.inputs.positionalMinCount != null) ? descr.inputs.positionalMinCount : 1;
            for (int i = 0; i < cnt; i++) {
                String name = verb + "_" + i;

                List<String> columns = null;
                if (descr.inputs.positional.columnBased) {
                    columns = new ArrayList<>();

                    List<String> named = namedColumns.get(null);
                    if ((named != null) && !named.isEmpty()) {
                        columns.addAll(named);
                    } else {
                        columns.add("<* A list of columns *>");
                    }
                }

                createSourceOps(name, dsNum, st, columns, task, input);
                posInputs.add(name);
            }

            opDef.inputs.positionalNames = String.join(",", posInputs);
        } else if (descr.inputs.named != null) {
            opDef.inputs.named = TaskDefinitionLanguage.createDefinitions(task);
            Arrays.stream(descr.inputs.named).forEach(nsDesc -> {
                StreamType st = nsDesc.types[0];

                List<String> columns = null;
                if (nsDesc.columnBased) {
                    columns = new ArrayList<>();

                    List<String> named = namedColumns.get(nsDesc.name);
                    if ((named != null) && !named.isEmpty()) {
                        columns.addAll(named);
                    } else {
                        columns.add("<* A list of columns *>");
                    }
                }

                createSourceOps(verb + "_" + nsDesc.name, dsNum, st, columns, task, input);

                opDef.inputs.named.put(nsDesc.name, verb + "_" + nsDesc.name);
            });
        }

        task.taskItems.add(opDef);

        if (descr.outputs.positional != null) {
            StreamType st = descr.outputs.positional.types[0];

            List<String> columns = null;
            if (descr.outputs.positional.columnBased) {
                columns = new ArrayList<>();

                if ((descr.outputs.positional.generatedColumns != null) && (descr.outputs.positional.generatedColumns.length > 0)) {
                    columns.addAll(Arrays.asList(descr.outputs.positional.generatedColumns));
                } else {
                    columns.add("<* A list of columns from input(s) goes here *>");
                }
            }

            createOutput(verb, dsNum, st, columns, task, output);

            opDef.outputs.positionalNames = verb;
        } else if (descr.outputs.named != null) {
            opDef.outputs.named = TaskDefinitionLanguage.createDefinitions(task);
            Arrays.stream(descr.outputs.named).forEach(nsDesc -> {
                StreamType st = nsDesc.types[0];

                List<String> columns = null;
                if (nsDesc.columnBased) {
                    columns = new ArrayList<>();

                    if ((nsDesc.generatedColumns != null) && (nsDesc.generatedColumns.length > 0)) {
                        columns.addAll(Arrays.asList(nsDesc.generatedColumns));
                    } else {
                        columns.add("<* A list of columns from input(s) goes here *>");
                    }
                }

                createOutput(nsDesc.name, dsNum, st, columns, task, output);

                opDef.outputs.named.put(nsDesc.name, nsDesc.name);
            });
        }

        TaskDefinitionLanguage.DataStream defDs = task.dataStreams.get(Constants.DEFAULT_DS);
        defDs.output = new TaskDefinitionLanguage.StreamDesc();
        defDs.output.path = "proto://full/path/to/output/directory";

        task.prefix = prefix;
        task.input = input;
        task.output = output;

        return task;
    }

    private static Map<String, String> parameterListToMap(List<TaskDocumentationLanguage.Parameter> pl) {
        return pl.stream().collect(Collectors.toMap(l -> l.name, l -> l.descr));
    }

    private static void createSourceOps(String name, AtomicInteger dsNum, StreamType st, List<String> columns, TaskDefinitionLanguage.Task task, List<String> input) {
        switch (st) {
            case Plain:
            case CSV: {
                TaskDefinitionLanguage.DataStream inp = new TaskDefinitionLanguage.DataStream();
                if (columns != null) {
                    inp.input = new TaskDefinitionLanguage.StreamDesc();
                    inp.input.columns = String.join(Constants.COMMA, columns);
                    inp.input.delimiter = ",";
                    inp.input.path = "proto://full/path/to/source_" + dsNum + "/*.csv";
                    inp.input.partCount = "100500";
                }

                task.dataStreams.put(name, inp);
                input.add(name);

                break;
            }
            case Point: {
                TaskDefinitionLanguage.Operation src = TaskDefinitionLanguage.createOperation(task);
                src.verb = PointCSVSourceOperation.VERB;
                src.name = "source_" + dsNum.incrementAndGet();
                src.inputs.positionalNames = "source_" + dsNum;
                src.outputs.positionalNames = name;
                src.definitions = TaskDefinitionLanguage.createDefinitions(task);
                src.definitions.put(ConfigurationParameters.DS_CSV_RADIUS_COLUMN, "source_" + dsNum + ".radius");
                src.definitions.put(ConfigurationParameters.DS_CSV_LAT_COLUMN, "source_" + dsNum + ".lat");
                src.definitions.put(ConfigurationParameters.DS_CSV_LON_COLUMN, "source_" + dsNum + ".lon");

                task.taskItems.add(src);

                TaskDefinitionLanguage.DataStream inp = new TaskDefinitionLanguage.DataStream();
                inp.input = new TaskDefinitionLanguage.StreamDesc();
                inp.input.columns = "lat,lon,radius";
                inp.input.path = "proto://full/path/to/source_" + dsNum + "/*.csv";
                inp.input.partCount = "100500";

                task.dataStreams.put("source_" + dsNum, inp);
                input.add("source_" + dsNum);

                TaskDefinitionLanguage.DataStream output = new TaskDefinitionLanguage.DataStream();
                if (columns != null) {
                    output.input = new TaskDefinitionLanguage.StreamDesc();
                    output.input.columns = String.join(Constants.COMMA, columns);
                    output.input.delimiter = ",";
                }

                task.dataStreams.put(name, output);

                break;
            }
            case Track: {
                TaskDefinitionLanguage.Operation src = TaskDefinitionLanguage.createOperation(task);
                src.verb = TrackGPXSourceOperation.VERB;
                src.name = "source_" + dsNum.incrementAndGet();
                src.inputs.positionalNames = "source_" + dsNum;
                src.outputs.positionalNames = name;

                task.taskItems.add(src);

                TaskDefinitionLanguage.DataStream inp = new TaskDefinitionLanguage.DataStream();
                inp.input = new TaskDefinitionLanguage.StreamDesc();
                inp.input.path = "proto://full/path/to/source_" + dsNum + "/*.gpx";
                inp.input.partCount = "100500";

                task.dataStreams.put("source_" + dsNum, inp);
                input.add("source_" + dsNum);

                TaskDefinitionLanguage.DataStream output = new TaskDefinitionLanguage.DataStream();
                if (columns != null) {
                    output.input = new TaskDefinitionLanguage.StreamDesc();
                    output.input.columns = String.join(Constants.COMMA, columns);
                    output.input.delimiter = ",";
                }

                task.dataStreams.put(name, output);

                break;
            }
            case Polygon: {
                TaskDefinitionLanguage.Operation src = TaskDefinitionLanguage.createOperation(task);
                src.verb = PolygonJSONSourceOperation.VERB;
                src.name = "source_" + dsNum.incrementAndGet();
                src.inputs.positionalNames = "source_" + dsNum;
                src.outputs.positionalNames = name;

                task.taskItems.add(src);

                TaskDefinitionLanguage.DataStream inp = new TaskDefinitionLanguage.DataStream();
                inp.input = new TaskDefinitionLanguage.StreamDesc();
                inp.input.path = "proto://full/path/to/source_" + dsNum + "/*.json";
                inp.input.partCount = "100500";

                task.dataStreams.put("source_" + dsNum, inp);
                input.add("source_" + dsNum);

                TaskDefinitionLanguage.DataStream output = new TaskDefinitionLanguage.DataStream();
                if (columns != null) {
                    output.input = new TaskDefinitionLanguage.StreamDesc();
                    output.input.columns = String.join(Constants.COMMA, columns);
                    output.input.delimiter = ",";
                }

                task.dataStreams.put(name, output);

                break;
            }
            case KeyValue: {
                TaskDefinitionLanguage.DataStream inter = new TaskDefinitionLanguage.DataStream();
                inter.input = new TaskDefinitionLanguage.StreamDesc();
                inter.input.columns = "key,_";
                inter.input.delimiter = ",";
                inter.input.path = "proto://full/path/to/source_" + dsNum + "/*.csv";
                inter.input.partCount = "100500";

                task.dataStreams.put(name + "_0", inter);
                input.add(name + "_0");

                TaskDefinitionLanguage.Operation toPair = TaskDefinitionLanguage.createOperation(task);
                toPair.verb = MapToPairOperation.VERB;
                toPair.name = "source_" + dsNum.incrementAndGet();
                toPair.inputs.positionalNames = name + "_0";
                toPair.outputs.positionalNames = name;
                toPair.definitions = TaskDefinitionLanguage.createDefinitions(task);
                toPair.definitions.put(MapToPairOperation.OP_KEY_COLUMNS, name + "_0.key");

                task.taskItems.add(toPair);

                TaskDefinitionLanguage.DataStream output = new TaskDefinitionLanguage.DataStream();
                if (columns != null) {
                    output.input = new TaskDefinitionLanguage.StreamDesc();
                    output.input.columns = String.join(Constants.COMMA, columns);
                    output.input.delimiter = ",";
                }

                task.dataStreams.put(name, output);

                break;
            }
        }
    }

    private static void createOutput(String name, AtomicInteger dsNum, StreamType st, List<String> columns, TaskDefinitionLanguage.Task task, List<String> outputs) {
        switch (st) {
            case Point: {
                TaskDefinitionLanguage.Operation out = TaskDefinitionLanguage.createOperation(task);
                out.verb = PointCSVOutputOperation.VERB;
                out.name = "output_" + dsNum.incrementAndGet();
                out.inputs.positionalNames = name;
                out.outputs.positionalNames = name + "_output";

                task.taskItems.add(out);

                TaskDefinitionLanguage.DataStream output = new TaskDefinitionLanguage.DataStream();
                if (columns != null) {
                    output.output = new TaskDefinitionLanguage.StreamDesc();
                    output.output.columns = String.join(Constants.COMMA, columns);
                    output.output.delimiter = ",";
                }

                task.dataStreams.put(name + "_output", output);
                outputs.add(name + "_output");
                break;
            }
            case Track: {
                TaskDefinitionLanguage.Operation out = TaskDefinitionLanguage.createOperation(task);
                out.verb = TrackGPXOutputOperation.VERB;
                out.name = "output_" + dsNum.incrementAndGet();
                out.inputs.positionalNames = name;
                out.outputs.positionalNames = name + "_output";

                task.taskItems.add(out);

                TaskDefinitionLanguage.DataStream output = new TaskDefinitionLanguage.DataStream();
                output.output = new TaskDefinitionLanguage.StreamDesc();
                output.output.columns = name + ".lat," + name + ".lon," + name + ".radius";
                output.output.delimiter = ",";

                task.dataStreams.put(name + "_output", output);
                outputs.add(name + "_output");
                break;
            }
            case Polygon: {
                TaskDefinitionLanguage.Operation out = TaskDefinitionLanguage.createOperation(task);
                out.verb = PolygonJSONOutputOperation.VERB;
                out.name = "output_" + dsNum.incrementAndGet();
                out.inputs.positionalNames = name;
                out.outputs.positionalNames = name + "_output";

                task.taskItems.add(out);

                TaskDefinitionLanguage.DataStream output = new TaskDefinitionLanguage.DataStream();
                if (columns != null) {
                    output.output = new TaskDefinitionLanguage.StreamDesc();
                    output.output.columns = String.join(Constants.COMMA, columns);
                    output.output.delimiter = ",";
                }

                task.dataStreams.put(name + "_output", output);
                outputs.add(name + "_output");
                break;
            }
            default: {
                TaskDefinitionLanguage.DataStream output = new TaskDefinitionLanguage.DataStream();
                if (columns != null) {
                    output.output = new TaskDefinitionLanguage.StreamDesc();
                    output.output.columns = String.join(Constants.COMMA, columns);
                    output.output.delimiter = ",";
                }

                task.dataStreams.put(name, output);
                outputs.add(name);
                break;
            }
        }
    }

    public static void packageDoc(String pkgName, Writer writer) throws Exception {
        String descr = Packages.getRegisteredPackages().get(pkgName);

        TaskDocumentationLanguage.Package pkg = packageDoc(pkgName, descr);

        VelocityContext ic = new VelocityContext();
        ic.put("pkg", pkg);

        Template index = Velocity.getTemplate("package.vm", UTF_8.name());
        index.merge(ic, writer);
    }

    public static TaskDocumentationLanguage.Package packageDoc(String pkgName, String descr) throws Exception {
        TaskDocumentationLanguage.Package pkg = new TaskDocumentationLanguage.Package(pkgName, descr);

        List<TaskDocumentationLanguage.Pair> ops = pkg.ops;

        for (Map.Entry<String, OpInfo> oi : Operations.getAvailableOperations(pkgName).entrySet()) {
            Method verb = oi.getValue().opClass.getDeclaredMethod("verb");
            Description d = verb.getDeclaredAnnotation(Description.class);

            ops.add(new TaskDocumentationLanguage.Pair(oi.getKey(), d.value()));
        }

        return pkg;
    }

    public static void indexDoc(Map<String, String> packages, FileWriter writer) {
        List<TaskDocumentationLanguage.Pair> pkgs = new ArrayList<>();

        for (Map.Entry<String, String> pkg : packages.entrySet()) {
            pkgs.add(new TaskDocumentationLanguage.Pair(pkg.getKey(), pkg.getValue()));
        }

        VelocityContext ic = new VelocityContext();
        ic.put("pkgs", pkgs);

        Template index = Velocity.getTemplate("index.vm", UTF_8.name());
        index.merge(ic, writer);
    }

    public static TaskDocumentationLanguage.Operation operationDoc(OpInfo opInfo) throws Exception {
        TaskDocumentationLanguage.Operation opDoc = new TaskDocumentationLanguage.Operation();

        opDoc.verb = opInfo.verb;

        Class<? extends Operation> operationClass = opInfo.opClass;

        Method verb = operationClass.getDeclaredMethod("verb");
        Description d = verb.getDeclaredAnnotation(Description.class);
        opDoc.descr = d.value();

        opDoc.pkg = operationClass.getPackage().getName();

        Descriptions ds = Descriptions.inspectOperation(operationClass);

        TaskDescriptionLanguage.Operation descr = opInfo.description;

        List<TaskDocumentationLanguage.Parameter> mandatoryParameters = opDoc.mandatoryParameters;
        List<TaskDocumentationLanguage.Parameter> optionalParameters = opDoc.optionalParameters;
        List<TaskDocumentationLanguage.Parameter> dynamicParameters = opDoc.dynamicParameters;
        if (descr.definitions != null) {
            for (TaskDescriptionLanguage.DefBase db : descr.definitions.values()) {
                TaskDocumentationLanguage.Parameter param;

                Field f;
                if (db instanceof TaskDescriptionLanguage.Definition) {
                    TaskDescriptionLanguage.Definition def = (TaskDescriptionLanguage.Definition) db;

                    f = ds.fields.get(ds.definitions.get(def.name));
                    d = f.getDeclaredAnnotation(Description.class);

                    param = new TaskDocumentationLanguage.Parameter(def.name, d.value());

                    param.type = def.clazz.getSimpleName();
                    if (def.clazz.isEnum()) {
                        param.values = getEnumDocs(def.clazz);
                    }

                    if (def.optional) {
                        String strippedName = ds.definitions.get(def.name)
                                .replaceFirst("^DS_", "")
                                .replaceFirst("^OP_", "");

                        f = ds.fields.get("DEF_" + strippedName);
                        d = f.getDeclaredAnnotation(Description.class);

                        param.defaults = new TaskDocumentationLanguage.Pair(def.defaults, d.value());

                        optionalParameters.add(param);
                    } else {
                        mandatoryParameters.add(param);
                    }
                } else {
                    TaskDescriptionLanguage.DynamicDef dyn = (TaskDescriptionLanguage.DynamicDef) db;

                    f = ds.fields.get(ds.definitions.get(dyn.name));
                    d = f.getDeclaredAnnotation(Description.class);

                    param = new TaskDocumentationLanguage.Parameter(dyn.name, d.value());

                    param.type = dyn.clazz.getSimpleName();
                    if (dyn.clazz.isEnum()) {
                        param.values = getEnumDocs(dyn.clazz);
                    }

                    dynamicParameters.add(param);
                }
            }
        }

        List<TaskDocumentationLanguage.Input> namedInputs = opDoc.namedInputs;
        if (descr.inputs.positional != null) {
            TaskDocumentationLanguage.Input input = new TaskDocumentationLanguage.Input(null, null);

            input.type = Arrays.stream(descr.inputs.positional.types)
                    .map(StreamType::name)
                    .collect(Collectors.toList());
            input.columnar = descr.inputs.positional.columnBased;

            opDoc.positionalInputs = input;
            opDoc.positionalMin = descr.inputs.positionalMinCount;
        } else if (descr.inputs.named != null) {
            Arrays.stream(descr.inputs.named)
                    .forEach(ns -> {
                        Field f = ds.fields.get(ds.inputs.get(ns.name));
                        Description de = f.getDeclaredAnnotation(Description.class);

                        TaskDocumentationLanguage.Input input = new TaskDocumentationLanguage.Input(ns.name, de.value());

                        input.type = Arrays.stream(ns.types)
                                .map(StreamType::name)
                                .collect(Collectors.toList());
                        input.columnar = ns.columnBased;

                        namedInputs.add(input);
                    });
        }

        List<TaskDocumentationLanguage.Output> namedOutputs = opDoc.namedOutputs;
        if (descr.outputs.positional != null) {
            TaskDocumentationLanguage.Output output = new TaskDocumentationLanguage.Output(null, null);

            output.type = Arrays.stream(descr.outputs.positional.types)
                    .map(StreamType::name)
                    .collect(Collectors.toList());
            output.columnar = descr.outputs.positional.columnBased;

            output.generated = new ArrayList<>();
            if (descr.outputs.positional.generatedColumns != null) {
                Arrays.stream(descr.outputs.positional.generatedColumns)
                        .forEach(gen -> {
                            Field fG = ds.fields.get(ds.generated.get(gen));
                            Description deG = fG.getDeclaredAnnotation(Description.class);

                            output.generated.add(new TaskDocumentationLanguage.Pair(gen, deG.value()));
                        });
            }

            opDoc.positionalOutputs = output;
        } else if (descr.outputs.named != null) {
            Arrays.stream(descr.outputs.named)
                    .forEach(ns -> {
                        Field f = ds.fields.get(ds.outputs.get(ns.name));
                        Description de = f.getDeclaredAnnotation(Description.class);

                        TaskDocumentationLanguage.Output output = new TaskDocumentationLanguage.Output(ns.name, de.value());

                        output.type = Arrays.stream(ns.types)
                                .map(StreamType::name)
                                .collect(Collectors.toList());
                        output.columnar = ns.columnBased;

                        output.generated = new ArrayList<>();
                        if (ns.generatedColumns != null) {
                            Arrays.stream(ns.generatedColumns)
                                    .forEach(gen -> {
                                        Field fG = ds.fields.get(ds.generated.get(gen));
                                        Description deG = fG.getDeclaredAnnotation(Description.class);

                                        output.generated.add(new TaskDocumentationLanguage.Pair(gen, deG.value()));
                                    });
                        }

                        namedOutputs.add(output);
                    });
        }

        return opDoc;
    }

    private static List<TaskDocumentationLanguage.Pair> getEnumDocs(Class<? extends Enum<?>> clazz) throws Exception {
        List<TaskDocumentationLanguage.Pair> enDescr = new ArrayList<>();

        for (Enum<?> e : clazz.getEnumConstants()) {
            Description d = e.getClass().getField(e.name()).getDeclaredAnnotation(Description.class);
            enDescr.add(new TaskDocumentationLanguage.Pair(e.name(), d.value()));
        }

        return enDescr;
    }

}
