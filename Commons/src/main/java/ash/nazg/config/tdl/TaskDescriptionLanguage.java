/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.config.tdl;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.collect.ObjectArrays.newArray;

public class TaskDescriptionLanguage {
    private static <T> T[] arrcat(Class<T> type, T[] first, T[] second) {
        int secondLength = (second == null) ? 0 : second.length;
        int firstLength = (first == null) ? 0 : first.length;
        if ((first != null) || (second != null)) {
            T[] result = newArray(type, firstLength + secondLength);
            if (first != null) {
                System.arraycopy(first, 0, result, 0, firstLength);
            }
            if (second != null) {
                System.arraycopy(second, 0, result, firstLength, secondLength);
            }
            return result;
        }
        return null;
    }

    public enum StreamType {
        /**
         * This type represents the case if any other non-special RDD types except {@link #KeyValue} are allowed
         */
        Plain,
        /**
         * The underlying CSV RDD a collection of rows with a flexibly defined set of columns
         */
        CSV,
        /**
         * For output CSV RDDs with a fixed set of columns
         */
        Fixed,
        /**
         * For RDDs consisting of Point objects
         */
        Point,
        /**
         * For RDDs consisting of Polygon objects
         */
        Polygon,
        /**
         * For PairRDDs, each record of whose is a key and value pair
         */
        KeyValue,
        /**
         * This special type is allowed only for the output of filter-like operations that have exactly one positional
         * input (of any type), and just throw out some records as a whole, never changing records that pass the filter
         */
        Passthru
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Operation {
        @JsonProperty(required = true, value = "verb")
        public final String verb;

        @JsonProperty(value = "definitions")
        public final DefBase[] definitions;

        @JsonProperty(value = "inputs")
        public final OpStreams inputs;

        @JsonProperty(value = "outputs")
        public final OpStreams outputs;

        public Operation(String verb, DefBase[] definitions, OpStreams inputs, OpStreams outputs) {
            this.definitions = definitions;
            this.inputs = inputs;
            this.outputs = outputs;
            this.verb = verb;
        }

        public Operation merge(Operation operation) {
            OpStreams inputs = null;
            if (operation.inputs != null) {
                if (this.inputs != null) {
                    if ((this.inputs.positional != null) || (operation.inputs.positional != null)) {
                        DataStream positional = new DataStream(
                                arrcat(StreamType.class,
                                        (operation.inputs.positional != null) ? operation.inputs.positional.types : null,
                                        (this.inputs.positional != null) ? this.inputs.positional.types : null
                                ),
                                ((this.inputs.positional != null) && this.inputs.positional.columnBased)
                                        || ((operation.inputs.positional != null) && operation.inputs.positional.columnBased)
                        );

                        inputs = new OpStreams(positional);
                    } else if ((this.inputs.named != null) || (operation.inputs.named != null)) {
                        Map<String, NamedStream> inpsOver = new HashMap<>();
                        if (operation.inputs.named != null) {
                            inpsOver = Arrays.stream(operation.inputs.named).collect(Collectors.toMap(inp -> inp.name, inp -> inp));
                        }
                        Map<String, NamedStream> inps = new HashMap<>();
                        if (this.inputs.named != null) {
                            inps = Arrays.stream(this.inputs.named).collect(Collectors.toMap(inp -> inp.name, inp -> inp));
                        }
                        inps.putAll(inpsOver);

                        inputs = inps.size() == 0 ? null : new OpStreams(inps.values().toArray(new NamedStream[0]));
                    }
                } else {
                    inputs = operation.inputs;
                }
            } else {
                inputs = this.inputs;
            }

            OpStreams outputs = null;
            if (operation.outputs != null) {
                if (this.outputs != null) {
                    if ((this.outputs.positional != null) || (operation.outputs.positional != null)) {
                        DataStream positional = new DataStream(
                                arrcat(StreamType.class,
                                        (operation.outputs.positional != null) ? operation.outputs.positional.types : null,
                                        (this.outputs.positional != null) ? this.outputs.positional.types : null
                                ),
                                ((this.outputs.positional != null) && this.outputs.positional.columnBased)
                                        || ((operation.outputs.positional != null) && operation.outputs.positional.columnBased)
                        );

                        outputs = new OpStreams(positional);
                    } else if ((this.outputs.named != null) || (operation.outputs.named != null)) {
                        Map<String, NamedStream> outsOver = new HashMap<>();
                        if (operation.outputs.named != null) {
                            outsOver = Arrays.stream(operation.outputs.named).collect(Collectors.toMap(out -> out.name, out -> out));
                        }
                        Map<String, NamedStream> outs = new HashMap<>();
                        if (this.outputs.named != null) {
                            outs = Arrays.stream(this.outputs.named).collect(Collectors.toMap(out -> out.name, out -> out));
                        }
                        outs.putAll(outsOver);

                        outputs = outs.size() == 0 ? null : new OpStreams(outs.values().toArray(new NamedStream[0]));
                    }
                } else {
                    outputs = operation.outputs;
                }
            } else {
                outputs = this.outputs;
            }

            Map<String, DefBase> defOver = new HashMap<>();
            if (operation.definitions != null) {
                defOver = Arrays.stream(operation.definitions).collect(Collectors.toMap(d -> d instanceof Definition ? ((Definition) d).name : ((DynamicDef) d).prefix, d -> d));
            }
            Map<String, DefBase> defs = new HashMap<>();
            if (definitions != null) {
                defs = Arrays.stream(definitions).collect(Collectors.toMap(d -> d instanceof Definition ? ((Definition) d).name : ((DynamicDef) d).prefix, d -> d));
            }
            defs.putAll(defOver);

            return new Operation(verb,
                    defs.size() == 0 ? null : defs.values().toArray(new DefBase[0]),
                    inputs,
                    outputs
            );
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class OpStreams {
        @JsonProperty(value = "positional")
        public final DataStream positional;

        @JsonProperty(value = "positionalCount")
        public final Integer positionalMinCount;

        @JsonProperty(value = "named")
        public final NamedStream[] named;

        public OpStreams(DataStream positional) {
            this.positional = positional;
            this.named = null;
            this.positionalMinCount = null;
        }

        public OpStreams(DataStream positional, int positionalMinCount) {
            this.positional = positional;
            this.named = null;
            this.positionalMinCount = positionalMinCount;
        }

        public OpStreams(NamedStream[] named) {
            this.positional = null;
            this.named = named;
            this.positionalMinCount = null;
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class DataStream {
        @JsonProperty(value = "types")
        public final StreamType[] types;

        @JsonProperty(value = "columnBased")
        public final boolean columnBased;

        @JsonProperty(value = "generatedColumns")
        public final String[] generatedColumns;

        public DataStream(StreamType[] types, boolean columnBased) {
            this.types = types;
            this.columnBased = columnBased;
            this.generatedColumns = null;
        }

        public DataStream(StreamType[] types, String[] generatedColumns) {
            this.types = types;
            this.columnBased = true;
            this.generatedColumns = generatedColumns;
        }

        public DataStream(StreamType[] types, Collection<String> generatedColumns) {
            this.types = types;
            this.columnBased = true;
            this.generatedColumns = generatedColumns.toArray(new String[0]);
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class NamedStream extends DataStream {
        @JsonProperty(value = "name")
        public final String name;

        public NamedStream(String name, StreamType[] types, boolean columnBased) {
            super(types, columnBased);
            this.name = name;
        }

        public NamedStream(String name, StreamType[] types, String[] generatedColumns) {
            super(types, generatedColumns);
            this.name = name;
        }

        public NamedStream(String name, StreamType[] types, Collection<String> generatedColumns) {
            super(types, generatedColumns);
            this.name = name;
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class DefBase {
        @JsonProperty(required = true, value = "type")
        public final String type;

        @JsonProperty(value = "enumValues")
        public final String[] enumValues;

        @JsonIgnore
        public final Class clazz;

        public DefBase(Class clazz) {
            this.clazz = clazz;
            if (clazz.isEnum()) {
                this.type = Enum.class.getSimpleName();
                this.enumValues = Arrays.stream(clazz.getEnumConstants()).map(e -> ((Enum) e).name()).toArray(String[]::new);
            } else {
                this.type = clazz.getSimpleName();
                this.enumValues = null;
            }
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Definition extends DefBase {
        @JsonProperty(required = true, value = "name")
        public final String name;

        @JsonProperty(required = true, value = "optional")
        public final boolean optional;

        @JsonProperty(value = "defaults")
        public final String defaults;

        public <T> Definition(String name, Class<T> type, T defaults) {
            super(type);
            this.name = name;
            this.optional = true;
            this.defaults = (defaults != null) ? String.valueOf(defaults) : null;
        }

        public Definition(String name, Class<?> type) {
            super(type);
            this.name = name;
            this.optional = false;
            this.defaults = null;
        }

        public Definition(String name, Class<String[]> type, String[] defaults) {
            super(type);
            this.name = name;
            this.optional = true;
            this.defaults = (defaults != null) ? String.join(",", defaults) : null;
        }

        public Definition(String name, String[] defaults) {
            super(String[].class);
            this.name = name;
            this.optional = true;
            this.defaults = (defaults != null) ? String.join(",", defaults) : null;
        }

        public Definition(String name, String defaults) {
            super(String.class);
            this.name = name;
            this.optional = true;
            this.defaults = defaults;
        }

        public Definition(String name) {
            super(String.class);
            this.name = name;
            this.optional = false;
            this.defaults = null;
        }
    }

    public static class DynamicDef extends DefBase {
        @JsonProperty(required = true, value = "dynamic")
        public final boolean dynamic = true;

        @JsonProperty(required = true, value = "prefix")
        public final String prefix;

        public DynamicDef(String prefix, Class<?> type) {
            super(type);
            this.prefix = prefix;
        }
    }
}
