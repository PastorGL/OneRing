/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.simplefilters.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.Description;
import ash.nazg.config.tdl.StreamType;
import ash.nazg.config.tdl.TaskDescriptionLanguage;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;

import java.util.*;

import static ash.nazg.simplefilters.config.ConfigurationParameters.*;

@SuppressWarnings("unused")
public class ExactMatchFilterOperation extends MatchFilterOperation {
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
                        new TaskDescriptionLanguage.Definition(DS_SOURCE_MATCH_COLUMN),
                        new TaskDescriptionLanguage.Definition(OP_MATCH_VALUES, String[].class),
                },

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.NamedStream[]{
                                new TaskDescriptionLanguage.NamedStream(RDD_INPUT_SOURCE,
                                        new StreamType[]{StreamType.CSV},
                                        true
                                )
                        }
                ),

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.NamedStream[]{
                                new TaskDescriptionLanguage.NamedStream(RDD_OUTPUT_MATCHED,
                                        new StreamType[]{StreamType.CSV},
                                        true
                                ),
                                new TaskDescriptionLanguage.NamedStream(RDD_OUTPUT_EVICTED,
                                        new StreamType[]{StreamType.CSV},
                                        false
                                ),
                        }
                )
        );
    }

    @Override
    public void configure() throws InvalidConfigValueException {
        super.configure();

        matchSet = opResolver.definition(OP_MATCH_VALUES);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        JavaPairRDD<Boolean, Object> matched = ((JavaRDD<Object>) input.get(inputSourceName))
                .mapPartitionsToPair(new MatchFunction(
                        ctx.broadcast(new HashSet<>(Arrays.asList(matchSet))),
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