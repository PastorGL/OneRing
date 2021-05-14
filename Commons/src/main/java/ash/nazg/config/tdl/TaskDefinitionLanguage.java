/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.config.tdl;

import ash.nazg.config.InvalidConfigValueException;
import com.fasterxml.jackson.annotation.*;
import org.apache.commons.lang3.StringUtils;

import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import java.util.*;
import java.util.regex.Matcher;

public class TaskDefinitionLanguage {
    public static Operation createOperation(Task task) {
        Operation operation = new Operation();
        operation.inputs = new OpStreams();
        operation.inputs.task = task;
        operation.outputs = new OpStreams();
        operation.outputs.task = task;
        operation.task = task;
        return operation;
    }

    public static Directive createDirective(Task task, String directiveText) {
        Directive directive = new Directive();
        directive.task = task;
        directive.directive = directiveText;
        return directive;
    }

    public static Definitions createDefinitions(Task task) {
        Definitions definitions = new Definitions();
        definitions.task = task;
        return definitions;
    }

    public static Task createTask() {
        Task task = new Task();
        DataStreams dss = new DataStreams();
        DataStream defaultDs = new DataStream();
        defaultDs.input = new StreamDesc();
        defaultDs.output = new StreamDesc();
        dss.put(Constants.DEFAULT_DS, defaultDs);
        dss.task = task;
        task.dataStreams = dss;
        task.taskItems = new ArrayList<>();
        task.input = new ArrayList<>();
        task.output = new ArrayList<>();
        return task;
    }

    public static void fixup(Task task) {
        task.dataStreams.task = task;
        task.taskItems.forEach(ti -> {
            ti.task = task;
            if (ti instanceof Operation) {
                Operation op = (Operation) ti;
                if (op.definitions != null) {
                    op.definitions.task = task;
                }
                op.inputs.task = task;
                if (op.inputs.named != null) {
                    op.inputs.named.task = task;
                }
                op.outputs.task = task;
                if (op.outputs.named != null) {
                    op.outputs.named.task = task;
                }
            }
        });
        task.foreignLayer.values().forEach(l -> l.task = task);
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Task {
        @JsonProperty(required = true, value = "op")
        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        @Valid
        public List<TaskItem> taskItems;

        @JsonProperty(required = true, value = "ds")
        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        @Valid
        public DataStreams dataStreams;

        @JsonProperty(required = true, value = "input")
        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        public List<String> input;

        @JsonProperty(required = true, value = "output")
        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        public List<String> output;

        @JsonProperty(value = "prefix")
        public String prefix;

        @JsonProperty(value = "variables")
        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        public Map<String, String> variables;

        private final Map<String, Definitions> foreignLayer = new HashMap<>();

        @JsonAnySetter
        public void setForeignLayer(String key, String value) {
            String[] layer = key.split("\\.", 2);
            foreignLayer.compute(layer[0], (k, l) -> {
                if (l == null) {
                    l = new Definitions();
                    l.task = this;
                }
                l.put(layer[1], value);
                return l;
            });
        }

        @JsonAnyGetter
        public Map<String, String> getForeignLayer() {
            Map<String, String> ret = new HashMap<>();
            foreignLayer.forEach((k, v) -> v.keySet().forEach(kk -> ret.put(k + "." + kk, v.get(kk))));
            return ret;
        }

        @JsonIgnore
        public Set<String> foreignLayers() {
            return foreignLayer.keySet();
        }

        @JsonIgnore
        public Definitions foreignLayer(String layer) {
            Definitions layerDefs;
            if (foreignLayer.containsKey(layer)) {
                layerDefs = foreignLayer.get(layer);
            } else {
                layerDefs = new Definitions();
                layerDefs.task = this;
                foreignLayer.put(layer, layerDefs);
            }

            return layerDefs;
        }

        @JsonIgnore
        public void foreignLayer(String layer, Map layerProps) {
            Definitions layerDefs = foreignLayer.compute(layer, (k, l) -> {
                if (l == null) {
                    l = new Definitions();
                    l.task = this;
                }
                return l;
            });

            layerDefs.putAll(layerProps);
        }

        @JsonIgnore
        public String value(String value) {
            if (StringUtils.isEmpty(value)) {
                return null;
            }

            Matcher hasRepVar = Constants.REP_VAR.matcher(value);
            while (hasRepVar.find()) {
                String rep = hasRepVar.group(1);

                String repVar = rep;
                String repDef = null;
                if (rep.contains(Constants.REP_SEP)) {
                    String[] rd = rep.split(Constants.REP_SEP, 2);
                    repVar = rd[0];
                    repDef = rd[1];
                }

                String val = (variables != null) ? variables.getOrDefault(repVar, repDef) : repDef;

                if (val != null) {
                    value = value.replace("{" + rep + "}", val);
                }
            }

            return value;
        }

        @JsonIgnore
        public String getForeignValue(String key) {
            String[] layer = key.split("\\.", 2);
            if (foreignLayer.containsKey(layer[0])) {
                return foreignLayer.get(layer[0]).get(layer[1]);
            }

            return null;
        }

        @JsonIgnore
        public String[] arrayValue(String value) throws InvalidConfigValueException {
            String property = value(value);

            if (StringUtils.isEmpty(value)) {
                return null;
            }

            String[] strings = Arrays.stream(property.split(Constants.COMMA)).map(String::trim).filter(s -> !s.isEmpty()).toArray(String[]::new);
            return (strings.length == 0) ? null : strings;
        }
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.DEDUCTION)
    @JsonSubTypes({@JsonSubTypes.Type(Operation.class), @JsonSubTypes.Type(Directive.class)})
    public static abstract class TaskItem {
        @JsonIgnore
        protected Task task;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Operation extends TaskItem {
        @JsonProperty(required = true, value = "verb")
        @NotEmpty
        public String verb;

        @JsonProperty(value = "definitions")
        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        @Valid
        public Definitions definitions;

        @JsonProperty(value = "inputs")
        @Valid
        public OpStreams inputs;

        @JsonProperty(value = "outputs")
        @Valid
        public OpStreams outputs;

        @JsonProperty(required = true, value = "name")
        @NotEmpty
        public String name;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Directive extends TaskItem {
        @JsonProperty(required = true, value = "directive")
        @NotEmpty
        public String directive;

        @JsonIgnore
        public DirVarVal dirVarVal() {
            DirVarVal dvv;

            Matcher hasRepVar = Constants.REP_VAR.matcher(directive);
            if (hasRepVar.find()) {
                String rep = hasRepVar.group(1);

                String repVar = rep;
                String repDef = null;
                if (rep.contains(Constants.REP_SEP)) {
                    String[] rd = rep.split(Constants.REP_SEP, 2);
                    repVar = rd[0];
                    repDef = rd[1];
                }

                String val = task.value(repDef);

                dvv = new DirVarVal(directive.substring(1, hasRepVar.start(1) - 1), repVar, val);
            } else {
                dvv = new DirVarVal(directive.substring(1));
            }

            return dvv;
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class OpStreams {
        @JsonIgnore
        protected Task task;

        @JsonProperty(value = "named")
        @Valid
        public Definitions named;

        @JsonProperty(value = "positional")
        public String positionalNames;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @SuppressWarnings("unchecked")
    public static class Definitions extends HashMap<String, String> {
        @JsonIgnore
        protected Task task;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class DataStreams extends HashMap<String, DataStream> {
        @JsonIgnore
        protected Task task;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class DataStream {
        @JsonProperty(value = "input")
        @Valid
        public StreamDesc input;

        @JsonProperty(value = "output")
        @Valid
        public StreamDesc output;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class StreamDesc {
        @JsonProperty(value = "columns")
        public String columns;

        @JsonProperty(value = "delimiter")
        public String delimiter;

        @JsonProperty(value = "partCount")
        public String partCount;

        @JsonProperty(value = "path")
        public String path;
    }
}
