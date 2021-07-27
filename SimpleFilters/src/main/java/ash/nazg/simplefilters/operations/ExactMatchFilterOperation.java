/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.simplefilters.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.StreamType;
import ash.nazg.config.tdl.metadata.DefinitionMetaBuilder;
import ash.nazg.config.tdl.metadata.NamedStreamsMetaBuilder;
import ash.nazg.config.tdl.metadata.OperationMeta;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;

import java.util.*;

import static ash.nazg.simplefilters.config.ConfigurationParameters.*;

@SuppressWarnings("unused")
public class ExactMatchFilterOperation extends MatchFilterOperation {
    public static final String OP_MATCH_VALUES = "match.values";

    private String[] matchSet;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("exactMatch", "This operation is a filter that only passes the rows that have an exact match" +
                " with a specific set of allowed values in a given column",

                new NamedStreamsMetaBuilder()
                        .ds(RDD_INPUT_SOURCE, "CSV RDD with to be filtered",
                                new StreamType[]{StreamType.CSV}, true
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(DS_SOURCE_MATCH_COLUMN, "Column to match a value")
                        .def(OP_MATCH_VALUES, "Values to match any of them", String[].class)
                        .build(),

                new NamedStreamsMetaBuilder()
                        .ds(RDD_OUTPUT_MATCHED, "CSV RDD with matching values",
                                new StreamType[]{StreamType.CSV}, true
                        )
                        .ds(RDD_OUTPUT_EVICTED, "CSV RDD with non-matching values",
                                new StreamType[]{StreamType.CSV}
                        )
                        .build()
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