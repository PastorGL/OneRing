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
        operation.input = new OpStreams();
        operation.input.task = task;
        operation.output = new OpStreams();
        operation.output.task = task;
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
        task.streams = dss;
        task.items = new ArrayList<>();
        task.input = new ArrayList<>();
        task.output = new ArrayList<>();
        return task;
    }

    public static void fixup(Task task) {
        task.streams.task = task;
        task.items.forEach(ti -> {
            ti.task = task;
            if (ti instanceof Operation) {
                Operation op = (Operation) ti;
                if (op.definitions != null) {
                    op.definitions.task = task;
                }
                op.input.task = task;
                if (op.input.named != null) {
                    op.input.named.task = task;
                }
                op.output.task = task;
                if (op.output.named != null) {
                    op.output.named.task = task;
                }
            }
        });
        task.foreignLayer.values().forEach(l -> l.task = task);
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Task {
        @JsonProperty(required = true)
        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        @Valid
        public List<TaskItem> items;

        @JsonProperty(required = true)
        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        @Valid
        public DataStreams streams;

        @JsonProperty(required = true)
        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        public List<String> input;

        @JsonProperty(required = true)
        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        public List<String> output;

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        public String prefix;

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        public Map<String, String> variables;

        private final Map<String, Definitions> foreignLayer = new HashMap<>();

        @JsonAnySetter
        public void setForeignLayer(String key, String value) {
            String[] layer = key.split("\\.", 2);
            if (layer.length != 2) {
                throw new InvalidConfigValueException("Configuration key '" + key + "' doesn't adhere to 'layer.value' pattern. Check the syntax");
            }
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
            value = value(value);

            if (StringUtils.isEmpty(value)) {
                return null;
            }

            List<String> values = new ArrayList<>();
            int length = value.length();
            boolean inBrace = false;
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < length; i++) {
                char c = value.charAt(i);

                switch (c) {
                    case '{' : {
                        inBrace = true;
                        break;
                    }
                    case '}' : {
                        inBrace = false;
                        break;
                    }
                    case ',' : {
                        if (!inBrace && (sb.length() != 0)) {
                            values.add(sb.toString());
                            sb = new StringBuilder();
                            continue;
                        }
                    }
                }
                sb.append(c);
            }
            if (sb.length() != 0) {
                values.add(sb.toString());
            }

            return (values.size() == 0) ? null : values.toArray(new String[0]);
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
        @JsonProperty(required = true)
        @NotEmpty
        public String name;

        @JsonProperty(required = true)
        @NotEmpty
        public String verb;

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        @Valid
        public Definitions definitions;

        @JsonProperty(required = true)
        @Valid
        public OpStreams input;

        @JsonProperty(required = true)
        @Valid
        public OpStreams output;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Directive extends TaskItem {
        @JsonProperty(required = true)
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

        public Definitions named;

        public String positional;
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
        @Valid
        public StreamDesc input;

        @Valid
        public StreamDesc output;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class StreamDesc {
        public String columns;

        public String delimiter;

        public String partCount;
    }
}
