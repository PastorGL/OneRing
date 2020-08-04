package ash.nazg.simplefilters.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.Description;
import ash.nazg.config.tdl.TaskDescriptionLanguage;
import ash.nazg.spark.Operation;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVWriter;
import org.apache.hadoop.io.Text;
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

    private String inputValuesName;
    private char inputValuesDelimiter;
    private int valuesColumn;

    private char inputSourceDelimiter;
    private String inputSourceName;
    private int matchColumn;

    private String outputMatchedName;
    private String outputEvictedName;

    private char outputDelimiter;
    private int[] outputCols;

    @Override
    @Description("Takes a CSV RDD with hashed signals and CSV RDD with H3 hashes and filters signals by coverage")
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
                                new TaskDescriptionLanguage.NamedStream(
                                        RDD_INPUT_SOURCE,
                                        new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.CSV},
                                        true
                                ),
                                new TaskDescriptionLanguage.NamedStream(
                                        RDD_INPUT_VALUES,
                                        new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.CSV},
                                        true
                                ),
                        }
                ),

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.NamedStream[]{
                                new TaskDescriptionLanguage.NamedStream(RDD_OUTPUT_MATCHED,
                                        new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.CSV},
                                        true
                                ),
                                new TaskDescriptionLanguage.NamedStream(RDD_OUTPUT_EVICTED,
                                        new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.Passthru},
                                        false
                                ),
                        }
                )
        );
    }

    @Override
    public void configure(Properties config, Properties variables) throws InvalidConfigValueException {
        super.configure(config, variables);

        inputValuesName = describedProps.namedInputs.get(RDD_INPUT_VALUES);
        inputValuesDelimiter = dataStreamsProps.inputDelimiter(inputValuesName);
        inputSourceName = describedProps.namedInputs.get(RDD_INPUT_SOURCE);
        inputSourceDelimiter = dataStreamsProps.inputDelimiter(inputSourceName);

        outputMatchedName = describedProps.namedOutputs.get(RDD_OUTPUT_MATCHED);
        outputEvictedName = describedProps.namedOutputs.get(RDD_OUTPUT_EVICTED);

        Map<String, Integer> inputSignalsColumns = dataStreamsProps.inputColumns.get(inputSourceName);
        Map<String, Integer> inputGeometriesColumns = dataStreamsProps.inputColumns.get(inputValuesName);

        String prop;

        prop = describedProps.defs.getTyped(DS_SOURCE_MATCH_COLUMN);
        matchColumn = inputSignalsColumns.get(prop);

        prop = describedProps.defs.getTyped(DS_VALUES_MATCH_COLUMN);
        valuesColumn = inputSignalsColumns.get(prop);

        outputDelimiter = dataStreamsProps.outputDelimiter(outputMatchedName);

        String[] outputColumns = dataStreamsProps.outputColumns.get(outputMatchedName);
        outputCols = new int[outputColumns.length];
        for (int i = 0; i < outputColumns.length; i++) {
            String col = outputColumns[i];
            outputCols[i] = col.startsWith(inputValuesName)
                    ? -inputGeometriesColumns.get(col) - 1
                    : +inputSignalsColumns.get(col);
        }
    }

    @Override
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        char _inputSourceDelimiter = inputSourceDelimiter;
        char _inputValuesDelimiter = inputValuesDelimiter;
        int _matchColumn = matchColumn;
        int _valuesColumn = valuesColumn;

        char _outputDelimiter = outputDelimiter;
        int[] _outputCols = outputCols;

        JavaRDD<Object> inputSource = (JavaRDD<Object>) input.get(inputSourceName);

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

                        StringWriter buffer = new StringWriter();
                        CSVWriter writer = new CSVWriter(buffer, _outputDelimiter, CSVWriter.DEFAULT_QUOTE_CHARACTER,
                                CSVWriter.DEFAULT_ESCAPE_CHARACTER, "");
                        writer.writeNext(acc, false);
                        writer.close();

                        for (int i = 0; i < outputCols.length; i++) {
                            int c = outputCols[i];

                            acc[i] = (c < 0) ? "" : row[i];
                        }

                        ret.add(new Tuple2<>(source, new Text(buffer.toString())));
                    }

                    return ret.iterator();
                });

        int partCount = sourcePair.getNumPartitions();

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

                        for (int i = 0; i < outputCols.length; i++) {
                            int c = outputCols[i];

                            acc[i] = (c < 0) ? row[-i + 1] : "";
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
                .repartition(partCount);

        JavaPairRDD<Boolean, Text> signals = sourcePair
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

                            String[] acc = parser.parseLine(value);
                            String[] row = parser.parseLine(match);

                            for (int i = 0; i < outputCols.length; i++) {
                                int c = outputCols[i];

                                acc[i] = (c < 0) ? row[i] : acc[i];
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
            ret.put(outputMatchedName, signals.filter(t -> t._1).values());
            ret.put(outputEvictedName, signals.filter(t -> !t._1).values());

            return Collections.unmodifiableMap(ret);
        } else {
            return Collections.singletonMap(outputMatchedName, signals.filter(t -> t._1).values());
        }
    }
}
