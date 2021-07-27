/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.simplefilters.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.StreamType;
import ash.nazg.config.tdl.metadata.DefinitionMetaBuilder;
import ash.nazg.config.tdl.metadata.OperationMeta;
import ash.nazg.config.tdl.metadata.PositionalStreamsMetaBuilder;
import ash.nazg.spark.Operation;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

@SuppressWarnings("unused")
public class SkipLinesOperation extends Operation {
    public static final String OP_LINE_PATTERN = "line.pattern";
    public static final String OP_LINE_VALUE = "line.value";
    public static final String OP_REVERSE = "reverse";

    private String inputName;

    private String outputName;

    private boolean skip;
    private boolean byPattern;
    private String value;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("skipLines", "Treat input as opaque Text RDD, and skip all lines that conform" +
                " to a regex pattern (or an exact value). Or, do the opposite for the pattern: skip all non-conforming lines",

                new PositionalStreamsMetaBuilder()
                        .ds("Plain RDD",
                                new StreamType[]{StreamType.Plain}
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(OP_REVERSE, "For line pattern, reverse operation to skip non-conforming lines",
                                Boolean.class, "false", "By default, skip lines conforming to a pattern." +
                                        " If set to 'true', reverse this condition")
                        .def(OP_LINE_PATTERN, "Java regex to define line pattern",
                                null, "By default, regex pattern is null")
                        .def(OP_LINE_VALUE, "Exact value to define line to skip (used only if a regex pattern isn't set)",
                                null, "By default, exact skip value is null")
                        .build(),

                new PositionalStreamsMetaBuilder()
                        .ds("Filtered Plain RDD",
                                new StreamType[]{StreamType.Plain}
                        )
                        .build()
        );
    }

    @Override
    public void configure() throws InvalidConfigValueException {
        inputName = opResolver.positionalInput(0);
        outputName = opResolver.positionalOutput(0);

        Boolean reverse = opResolver.definition(OP_REVERSE);

        skip = !reverse;
        value = opResolver.definition(OP_LINE_PATTERN);
        if (skip && (value == null)) {
            value = opResolver.definition(OP_LINE_VALUE);

            if (value == null) {
                throw new InvalidConfigValueException("Operation '" + name + "' requires regex pattern or exact value");
            }

            byPattern = false;
        } else {
            if (value == null) {
                throw new InvalidConfigValueException("Operation '" + name + "' requires a regex pattern");
            }

            byPattern = true;
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        boolean _skip = skip;
        boolean _byPattern = byPattern;
        String _value = value;

        JavaRDD<Object> output = ((JavaRDD<Object>) input.get(inputName))
                .mapPartitions(it -> {
                    List<Object> ret = new ArrayList<>();

                    Pattern pattern = _byPattern ? Pattern.compile(_value) : null;

                    while (it.hasNext()) {
                        Object o = it.next();
                        String line = String.valueOf(o);

                        if (_skip) {
                            if (_byPattern) {
                                if (!pattern.matcher(line).matches()) {
                                    ret.add(o);
                                }
                            } else {
                                if (!_value.equals(line)) {
                                    ret.add(o);
                                }
                            }
                        } else {
                            if (pattern.matcher(line).matches()) {
                                ret.add(o);
                            }
                        }
                    }

                    return ret.iterator();
                });

        return Collections.singletonMap(outputName, output);
    }
}
