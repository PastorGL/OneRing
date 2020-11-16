/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.simplefilters.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.Description;
import ash.nazg.config.tdl.TaskDescriptionLanguage;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;

import java.util.*;

import static ash.nazg.simplefilters.config.ConfigurationParameters.*;

@SuppressWarnings("unused")
public class ListMatchFilterOperation extends MatchFilterOperation {
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
                        new TaskDescriptionLanguage.Definition(DS_SOURCE_MATCH_COLUMN),
                        new TaskDescriptionLanguage.Definition(DS_VALUES_MATCH_COLUMN),
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
                        new TaskDescriptionLanguage.NamedStream[]{
                                new TaskDescriptionLanguage.NamedStream(RDD_OUTPUT_MATCHED,
                                        new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.CSV},
                                        true
                                ),
                                new TaskDescriptionLanguage.NamedStream(RDD_OUTPUT_EVICTED,
                                        new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.CSV},
                                        false
                                ),
                        }
                )
        );
    }

    @Override
    public void configure(Properties properties, Properties variables) throws InvalidConfigValueException {
        super.configure(properties, variables);

        inputValuesName = describedProps.namedInputs.get(RDD_INPUT_VALUES);
        inputValuesDelimiter = dataStreamsProps.inputDelimiter(inputValuesName);

        Map<String, Integer> inputValuesColumns = dataStreamsProps.inputColumns.get(inputValuesName);

        String prop = describedProps.defs.getTyped(DS_VALUES_MATCH_COLUMN);
        valuesColumn = inputValuesColumns.get(prop);
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

        JavaPairRDD<Boolean, Object> matched = ((JavaRDD<Object>) input.get(inputSourceName))
                .mapPartitionsToPair(new MatchFunction(
                        ctx.broadcast(new HashSet<>(matchSet)),
                        inputSourceDelimiter,
                        matchColumn
                ));

        if (outputEvictedName != null) {
            Map<String, JavaRDDLike> ret = new HashMap<>();
            ret.put(outputMatchedName, matched.filter(t -> t._1).values());
            ret.put(outputEvictedName, matched.filter(t -> !t._1).values());

            return Collections.unmodifiableMap(ret);
        } else {
            return Collections.singletonMap(outputMatchedName, matched.filter(t -> t._1).values());
        }
    }
}