/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.simplefilters.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.Description;
import ash.nazg.config.tdl.TaskDescriptionLanguage;
import ash.nazg.config.OperationConfig;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;

import java.util.*;

@SuppressWarnings("unused")
public class ListMatchFilterOperation extends MatchFilterOperation {
    @Description("CSV RDD with to be filtered")
    public static final String RDD_INPUT_SOURCE = "source";
    @Description("CSV RDD with values to match any of them")
    public static final String RDD_INPUT_VALUES = "values";
    @Description("Column to match a value")
    public static final String DS_MATCH_COLUMN = "source.match.column";
    @Description("Column with a value to match")
    public static final String DS_VALUES_COLUMN = "values.match.column";

    public static final String VERB = "listMatch";

    private String inputValuesName;
    private char inputValuesDelimiter;
    private int valuesColumn;

    @Override
    @Description("This operation is a filter that only passes the RDD rows that have an exact match" +
            " with a specific set of allowed values in a given column sourced from another RDD's column")
    public String verb() {
        return VERB;
    }

    @Override
    public TaskDescriptionLanguage.Operation description() {
        return new TaskDescriptionLanguage.Operation(verb(),
                new TaskDescriptionLanguage.DefBase[]{
                        new TaskDescriptionLanguage.Definition(DS_MATCH_COLUMN),
                        new TaskDescriptionLanguage.Definition(DS_VALUES_COLUMN),
                },

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.NamedStream[]{
                                new TaskDescriptionLanguage.NamedStream(RDD_INPUT_SOURCE,
                                        new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.CSV},
                                        true
                                ),
                                new TaskDescriptionLanguage.NamedStream(RDD_INPUT_VALUES,
                                        new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.CSV},
                                        true
                                )
                        }
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
    public void setConfig(OperationConfig propertiesConfig) throws InvalidConfigValueException {
        super.setConfig(propertiesConfig);

        inputName = describedProps.namedInputs.get(RDD_INPUT_SOURCE);
        inputDelimiter = dataStreamsProps.inputDelimiter(inputName);

        Map<String, Integer> inputColumns = dataStreamsProps.inputColumns.get(inputName);

        String prop;
        prop = describedProps.defs.getTyped(DS_MATCH_COLUMN);
        matchColumn = inputColumns.get(prop);

        inputValuesName = describedProps.namedInputs.get(RDD_INPUT_VALUES);
        inputValuesDelimiter = dataStreamsProps.inputDelimiter(inputValuesName);

        inputColumns = dataStreamsProps.inputColumns.get(inputValuesName);

        prop = describedProps.defs.getTyped(DS_VALUES_COLUMN);
        valuesColumn = inputColumns.get(prop);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        char _inputValuesDelimiter = inputValuesDelimiter;
        int _valuesColumn = valuesColumn;

        List<String> matchSet = ((JavaRDD<Object>) input.get(inputValuesName))
                .mapPartitions(it -> {
                            CSVParser parser = new CSVParserBuilder().withSeparator(_inputValuesDelimiter).build();

                            Set<String> ret = new HashSet<>();
                            while (it.hasNext()) {
                                Object v = it.next();
                                String l = v instanceof String ? (String) v : String.valueOf(v);

                                String m = parser.parseLine(l)[_valuesColumn];

                                ret.add(m);
                            }

                            return ret.iterator();
                        }
                )
                .distinct()
                .collect();

        JavaRDD<Object> out = ((JavaRDD<Object>) input.get(inputName))
                .mapPartitions(new MatchFunction(
                        ctx.broadcast(new HashSet<>(matchSet)),
                        inputDelimiter,
                        matchColumn
                ));

        return Collections.singletonMap(outputName, out);
    }
}