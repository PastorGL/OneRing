/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.simplefilters.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.Description;
import ash.nazg.config.tdl.TaskDescriptionLanguage;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;

import java.util.*;

@SuppressWarnings("unused")
public class ExactMatchFilterOperation extends MatchFilterOperation {
    @Description("Column to match a value")
    public static final String DS_MATCH_COLUMN = "match.column";
    @Description("Values to match any of them")
    public static final String OP_MATCH_VALUES = "match.values";

    public static final String VERB = "exactMatch";

    private String[] matchSet;

    @Override
    @Description("This operation is a filter that only passes the rows that have an exact match" +
            " with a specific set of allowed values in a given column")
    public String verb() {
        return VERB;
    }

    @Override
    public TaskDescriptionLanguage.Operation description() {
        return new TaskDescriptionLanguage.Operation(verb(),
                new TaskDescriptionLanguage.DefBase[]{
                        new TaskDescriptionLanguage.Definition(DS_MATCH_COLUMN),
                        new TaskDescriptionLanguage.Definition(OP_MATCH_VALUES, String[].class),
                },

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.CSV},
                                true
                        )
                ),

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.Passthru},
                                false
                        )
                )
        );
    }

    @Override
    public void configure(Properties properties, Properties variables) throws InvalidConfigValueException {
        super.configure(properties, variables);

        inputName = describedProps.inputs.get(0);
        outputName = describedProps.outputs.get(0);

        inputDelimiter = dataStreamsProps.inputDelimiter(inputName);

        Map<String, Integer> inputColumns = dataStreamsProps.inputColumns.get(inputName);

        String prop;
        prop = describedProps.defs.getTyped(DS_MATCH_COLUMN);
        matchColumn = inputColumns.get(prop);

        matchSet = describedProps.defs.getTyped(OP_MATCH_VALUES);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        JavaRDD<Object> out = ((JavaRDD<Object>) input.get(inputName))
                .mapPartitions(new MatchFunction(
                        ctx.broadcast(new HashSet<>(Arrays.asList(matchSet))),
                        inputDelimiter,
                        matchColumn
                ));

        return Collections.singletonMap(outputName, out);
    }
}