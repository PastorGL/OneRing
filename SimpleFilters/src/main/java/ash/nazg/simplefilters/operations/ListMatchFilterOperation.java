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
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;

import java.util.*;

import static ash.nazg.simplefilters.config.ConfigurationParameters.*;

@SuppressWarnings("unused")
public class ListMatchFilterOperation extends MatchFilterOperation {
    private String inputValuesName;
    private char inputValuesDelimiter;
    private int valuesColumn;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("listMatch", "This operation is a filter that only passes the RDD rows that have an exact match" +
                " with a specific set of allowed values in a given column sourced from another RDD's column",

                new NamedStreamsMetaBuilder()
                        .ds(RDD_INPUT_SOURCE, "CSV RDD with to be filtered",
                                new StreamType[]{StreamType.CSV}, true
                        )
                        .ds(RDD_INPUT_VALUES, "CSV RDD with values to match any of them",
                                new StreamType[]{StreamType.CSV}, true
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(DS_SOURCE_MATCH_COLUMN, "Column to match a value")
                        .def(DS_VALUES_MATCH_COLUMN, "Column with a value to match")
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

        inputValuesName = opResolver.namedInput(RDD_INPUT_VALUES);
        inputValuesDelimiter = dsResolver.inputDelimiter(inputValuesName);

        Map<String, Integer> inputValuesColumns = dsResolver.inputColumns(inputValuesName);

        String prop = opResolver.definition(DS_VALUES_MATCH_COLUMN);
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