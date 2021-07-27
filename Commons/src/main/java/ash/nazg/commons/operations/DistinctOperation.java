/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.commons.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.StreamType;
import ash.nazg.config.tdl.metadata.DefinitionMetaBuilder;
import ash.nazg.config.tdl.metadata.OperationMeta;
import ash.nazg.config.tdl.metadata.PositionalStreamsMetaBuilder;
import ash.nazg.spark.Operation;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unused")
public class DistinctOperation extends Operation {
    public static final String DS_UNIQUE_COLUMN = "unique.column";

    private String inputName;
    private char inputDelimiter;

    private String outputName;

    private Integer uniqueColumn;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("distinct", "If input is an CSV RDD, this operation takes a column" +
                " and extract a list of distinct values occurred in this column. If input is a PairRDD, it returns" +
                " distinct elements from this PairRDD",

                new PositionalStreamsMetaBuilder()
                        .ds("Pair or CSV RDD",
                                new StreamType[]{StreamType.KeyValue, StreamType.CSV}, true
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(DS_UNIQUE_COLUMN, "Input column that contains a value treated as a distinction key",
                                String.class, null, "By default, no unique column is set for an RDD")
                        .build(),

                new PositionalStreamsMetaBuilder()
                        .ds("Same type as input RDD, but with uniquesced values",
                                new StreamType[]{StreamType.Passthru}
                        )
                        .build()
        );
    }

    @Override
    public void configure() throws InvalidConfigValueException {
        inputName = opResolver.positionalInput(0);
        inputDelimiter = dsResolver.inputDelimiter(inputName);
        outputName = opResolver.positionalOutput(0);

        String uniqueColumn = opResolver.definition(DS_UNIQUE_COLUMN);
        if (uniqueColumn != null) {
            Map<String, Integer> inputColumns = dsResolver.inputColumns(inputName);
            this.uniqueColumn = inputColumns.get(uniqueColumn);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        Object inp = input.get(inputName);

        if (inp instanceof JavaRDD) {
            char _inputDelimiter = inputDelimiter;
            int _uniqueColumn = uniqueColumn;

            JavaRDD<Text> out = ((JavaRDD<Object>) inp).mapPartitions(it -> {
                CSVParser parser = new CSVParserBuilder().withSeparator(_inputDelimiter).build();

                List<Text> ret = new ArrayList<>();
                while (it.hasNext()) {
                    Object v = it.next();
                    String l = v instanceof String ? (String) v : String.valueOf(v);

                    String[] ll = parser.parseLine(l);

                    ret.add(new Text(ll[_uniqueColumn]));
                }

                return ret.iterator();
            }).distinct();

            return Collections.singletonMap(outputName, out);
        }

        if (inp instanceof JavaPairRDD) {
            JavaPairRDD out = ((JavaPairRDD) inp)
                    .distinct();

            return Collections.singletonMap(outputName, out);
        }

        return null;
    }
}
