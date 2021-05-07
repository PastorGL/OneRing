/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.config.tdl;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

public class TaskDescriptionLanguage {
    public static class Operation {
        public final String verb;

        public final Map<String, DefBase> definitions;

        public final OpStreams inputs;
        public final OpStreams outputs;

        public Operation(String verb, DefBase[] definitions, OpStreams inputs, OpStreams outputs) {
            this.definitions = (definitions == null) ? null : Arrays.stream(definitions).collect(Collectors.toMap(d -> d.name, d -> d));
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
        public final String name;

        public final String type;

        public final String[] enumValues;

        public final Class clazz;

        public final boolean optional;

        public final String defaults;

        public final boolean dynamic;

        public DefBase(String name, Class clazz, boolean optional, String defaults, boolean dynamic) {
            this.name = name;
            this.clazz = clazz;
            if (clazz.isEnum()) {
                this.type = Enum.class.getSimpleName();
                this.enumValues = Arrays.stream(clazz.getEnumConstants()).map(e -> ((Enum) e).name()).toArray(String[]::new);
            } else {
                this.type = clazz.getSimpleName();
                this.enumValues = null;
            }
            this.optional = optional;
            this.defaults = defaults;
            this.dynamic = dynamic;
        }
    }

    public static class Definition extends DefBase {
        public <T> Definition(String name, Class<T> type, T defaults) {
            super(name, type, true, (defaults != null) ? String.valueOf(defaults) : null, false);
        }

        public Definition(String name, Class<?> type) {
            super(name, type, false, null, false);
        }

        public Definition(String name, Class<String[]> type, String[] defaults) {
            super(name, type, true, (defaults != null) ? String.join(",", defaults) : null, false);
        }

        public Definition(String name, String[] defaults) {
            super(name, String[].class, true, (defaults != null) ? String.join(",", defaults) : null, false);
        }

        public Definition(String name, String defaults) {
            super(name, String.class, true, defaults, false);
        }

        public Definition(String name) {
            super(name, String.class, false, null, false);
        }
    }

    public static class DynamicDef extends DefBase {
        public DynamicDef(String prefix, Class<?> type) {
            super(prefix, type, true, null, true);
        }
    }
}
