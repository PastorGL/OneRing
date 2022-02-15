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
import ash.nazg.spark.Operation;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVWriter;
import org.apache.hadoop.io.Text;
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import scala.Tuple2;

import java.io.StringWriter;
import java.util.*;

import static ash.nazg.simplefilters.config.ConfigurationParameters.*;

@SuppressWarnings("unused")
public class SplitMatchOperation extends Operation {
    public static final String VERB = "splitMatch";

    private char inputSourceDelimiter;
    private String inputSourceName;
    private int matchColumn;
    private String outputMatchedName;
    private String outputEvictedName;

    private String inputValuesName;
    private char inputValuesDelimiter;
    private int valuesColumn;

    private char outputDelimiter;
    private int[] outputCols;

    @Override
    public OperationMeta meta() {
        return new OperationMeta(VERB, "Takes a CSV RDD with values in a column and CSV RDD with tokens in another column," +
                " and augments rows of first by values from second for all token matches",

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
        inputSourceName = opResolver.namedInput(RDD_INPUT_SOURCE);
        inputSourceDelimiter = dsResolver.inputDelimiter(inputSourceName);

        outputMatchedName = opResolver.namedOutput(RDD_OUTPUT_MATCHED);
        outputEvictedName = opResolver.namedOutput(RDD_OUTPUT_EVICTED);

        Map<String, Integer> inputSourceColumns = dsResolver.inputColumns(inputSourceName);

        String prop;

        prop = opResolver.definition(DS_SOURCE_MATCH_COLUMN);
        matchColumn = inputSourceColumns.get(prop);

        inputValuesName = opResolver.namedInput(RDD_INPUT_VALUES);
        inputValuesDelimiter = dsResolver.inputDelimiter(inputValuesName);

        Map<String, Integer> inputValuesColumns = dsResolver.inputColumns(inputValuesName);

        prop = opResolver.definition(DS_VALUES_MATCH_COLUMN);
        valuesColumn = inputValuesColumns.get(prop);

        outputDelimiter = dsResolver.outputDelimiter(outputMatchedName);

        String[] outputColumns = dsResolver.outputColumns(outputMatchedName);
        outputCols = new int[outputColumns.length];
        for (int i = 0; i < outputColumns.length; i++) {
            String col = outputColumns[i];
            outputCols[i] = col.startsWith(inputValuesName)
                    ? -inputValuesColumns.get(col) - 1
                    : +inputSourceColumns.get(col);
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        char _inputSourceDelimiter = inputSourceDelimiter;
        char _inputValuesDelimiter = inputValuesDelimiter;
        int _matchColumn = matchColumn;
        int _valuesColumn = valuesColumn;

        char _outputDelimiter = outputDelimiter;
        int[] _outputCols = outputCols;

        JavaRDD<Object> inputSource = (JavaRDD<Object>) input.get(inputSourceName);

        int _numPartitions = inputSource.getNumPartitions();

        JavaPairRDD<Text, Text> sourcePair = inputSource
                .mapPartitionsToPair(it -> {
                    List<Tuple2<Text, Text>> ret = new ArrayList<>();

                    CSVParser parser = new CSVParserBuilder().withSeparator(_inputSourceDelimiter)
                            .build();

                    while (it.hasNext()) {
                        Object o = it.next();
                        String l = o instanceof String ? (String) o : String.valueOf(o);

                        String[] row = parser.parseLine(l);

                        Text source = new Text(row[_matchColumn]);

                        String[] acc = new String[_outputCols.length];

                        for (int i = 0; i < _outputCols.length; i++) {
                            int c = _outputCols[i];

                            acc[i] = (c < 0) ? "" : row[c];
                        }

                        StringWriter buffer = new StringWriter();
                        CSVWriter writer = new CSVWriter(buffer, _outputDelimiter, CSVWriter.DEFAULT_QUOTE_CHARACTER,
                                CSVWriter.DEFAULT_ESCAPE_CHARACTER, "");
                        writer.writeNext(acc, false);
                        writer.close();

                        ret.add(new Tuple2<>(source, new Text(buffer.toString())));
                    }

                    return ret.iterator();
                })
                .partitionBy(new HashPartitioner(_numPartitions));

        JavaRDD<Object> inputValues = (JavaRDD<Object>) input.get(inputValuesName);

        JavaPairRDD<Text, Text> valuesPair = inputValues
                .mapPartitionsToPair(it -> {
                    List<Tuple2<Text, Text>> ret = new ArrayList<>();

                    CSVParser parser = new CSVParserBuilder().withSeparator(_inputValuesDelimiter)
                            .build();

                    while (it.hasNext()) {
                        Object o = it.next();
                        String l = o instanceof String ? (String) o : String.valueOf(o);

                        String[] row = parser.parseLine(l);

                        Text value = new Text(row[_valuesColumn]);

                        String[] acc = new String[_outputCols.length];

                        for (int i = 0; i < _outputCols.length; i++) {
                            int c = _outputCols[i];

                            acc[i] = (c < 0) ? row[-1 - c] : "";
                        }

                        StringWriter buffer = new StringWriter();
                        CSVWriter writer = new CSVWriter(buffer, _outputDelimiter, CSVWriter.DEFAULT_QUOTE_CHARACTER,
                                CSVWriter.DEFAULT_ESCAPE_CHARACTER, "");
                        writer.writeNext(acc, false);
                        writer.close();

                        ret.add(new Tuple2<>(value, new Text(buffer.toString())));
                    }

                    return ret.iterator();
                })
                .partitionBy(new HashPartitioner(_numPartitions));

        JavaPairRDD<Boolean, Text> matched = sourcePair
                .zipPartitions(valuesPair, (itSource, itValues) -> {
                    List<Tuple2<Boolean, Text>> result = new ArrayList<>();

                    CSVParser parser = new CSVParserBuilder().withSeparator(_inputValuesDelimiter)
                            .build();

                    Map<Text, Text> values = new HashMap<>();
                    while (itValues.hasNext()) {
                        Tuple2<Text, Text> v = itValues.next();
                        values.put(v._1, v._2);
                    }

                    while (itSource.hasNext()) {
                        Tuple2<Text, Text> s = itSource.next();

                        Text source = s._1;

                        if (values.containsKey(source)) {
                            String match = values.get(source).toString();
                            String value = s._2.toString();

                            String[] row = parser.parseLine(match);
                            String[] acc = parser.parseLine(value);

                            for (int i = 0; i < _outputCols.length; i++) {
                                int c = _outputCols[i];

                                if (c < 0) {
                                    acc[i] = row[i];
                                }
                            }

                            StringWriter buffer = new StringWriter();
                            CSVWriter writer = new CSVWriter(buffer, _outputDelimiter, CSVWriter.DEFAULT_QUOTE_CHARACTER,
                                    CSVWriter.DEFAULT_ESCAPE_CHARACTER, "");
                            writer.writeNext(acc, false);
                            writer.close();

                            result.add(new Tuple2<>(true, new Text(buffer.toString())));
                        } else {
                            result.add(new Tuple2<>(false, s._2));
                        }
                    }

                    return result.iterator();
                })
                .mapToPair(t -> t);

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
