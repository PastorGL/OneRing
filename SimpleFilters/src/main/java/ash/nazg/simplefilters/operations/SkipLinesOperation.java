/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.simplefilters.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.Description;
import ash.nazg.config.tdl.TaskDescriptionLanguage;
import ash.nazg.spark.Operation;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;

import java.util.*;
import java.util.regex.Pattern;

@SuppressWarnings("unused")
public class SkipLinesOperation extends Operation {
    @Description("Java regex to define line pattern")
    public static final String OP_LINE_PATTERN = "line.pattern";
    @Description("Exact value to define line to skip (used only if a regex pattern isn't set)")
    public static final String OP_LINE_VALUE = "line.value";
    @Description("For line pattern, reverse operation to skip non-conforming lines")
    public static final String OP_REVERSE = "reverse";
    @Description("By default, skip lines conforming to a pattern. If set to 'true', reverse this condition")
    public static final Boolean DEF_REVERSE = false;
    @Description("By default, regex pattern is null")
    public static final String DEF_LINE_PATTERN = null;
    @Description("By default, exact skip value is null")
    public static final String DEF_LINE_VALUE = null;

    public static final String VERB = "skipLines";

    private String inputName;
    private String outputName;
    private boolean skip;
    private boolean byPattern;
    private String value;

    @Override
    @Description("Treat input as opaque Text RDD, and skip all lines that conform to a regex pattern (or an exact value)." +
            " Or, do the opposite for the pattern: skip all non-conforming lines")
    public String verb() {
        return VERB;
    }

    @Override
    public TaskDescriptionLanguage.Operation description() {
        return new TaskDescriptionLanguage.Operation(VERB,
                new TaskDescriptionLanguage.DefBase[]{
                        new TaskDescriptionLanguage.Definition(OP_REVERSE, Boolean.class, DEF_REVERSE),
                        new TaskDescriptionLanguage.Definition(OP_LINE_PATTERN, DEF_LINE_PATTERN),
                        new TaskDescriptionLanguage.Definition(OP_LINE_VALUE, DEF_LINE_VALUE),
                },

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.Plain},
                                false
                        )
                ),

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.Plain},
                                false
                        )
                )
        );
    }

    @Override
    public void configure(Properties config, Properties variables) throws InvalidConfigValueException {
        super.configure(config, variables);

        inputName = describedProps.inputs.get(0);
        outputName = describedProps.outputs.get(0);

        Boolean reverse = describedProps.defs.getTyped(OP_REVERSE);

        skip = !reverse;
        value = describedProps.defs.getTyped(OP_LINE_PATTERN);
        if (skip && (value == null)) {
            value = describedProps.defs.getTyped(OP_LINE_VALUE);

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
