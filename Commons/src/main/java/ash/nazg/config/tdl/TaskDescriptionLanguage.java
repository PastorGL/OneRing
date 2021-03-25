/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.config.tdl;

import java.util.Arrays;
import java.util.Collection;

public class TaskDescriptionLanguage {
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
         * For RDDs consisting of Track objects
         */
        Track,
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

    public static class Operation {
        public final String verb;

        public final DefBase[] definitions;

        public final OpStreams inputs;
        public final OpStreams outputs;

        public Operation(String verb, DefBase[] definitions, OpStreams inputs, OpStreams outputs) {
            this.definitions = definitions;
            this.inputs = inputs;
            this.outputs = outputs;
            this.verb = verb;
        }
    }

    public static class OpStreams {
        public final DataStream positional;

        public final Integer positionalMinCount;

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

    public static class DataStream {
        public final StreamType[] types;

        public final boolean columnBased;

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

    public static class NamedStream extends DataStream {
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

    public static class DefBase {
        public final String type;

        public final String[] enumValues;

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

    public static class Definition extends DefBase {
        public final String name;

        public final boolean optional;

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
        public final boolean dynamic = true;

        public final String prefix;

        public DynamicDef(String prefix, Class<?> type) {
            super(type);
            this.prefix = prefix;
        }
    }
}
