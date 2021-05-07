/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.commons.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.Description;
import ash.nazg.config.tdl.StreamType;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unused")
public class CollapsePairOperation extends Operation {
    private static final String VERB = "collapsePair";

    private String inputName;
    private String outputName;
    private char outputDelimiter;
    private int[] outputColumns;
    private char inputDelimiter;

    @Override
    @Description("Collapse Pair RDD into a CSV RDD")
    public String verb() {
        return VERB;
    }

    @Override
    public TaskDescriptionLanguage.Operation description() {
        return new TaskDescriptionLanguage.Operation(verb(),
                null,

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new StreamType[]{StreamType.KeyValue},
                                true
                        )
                ),

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new StreamType[]{StreamType.CSV},
                                true
                        )
                )
        );
    }

    @Override
    public void configure() throws InvalidConfigValueException {
        inputName = opResolver.positionalInput(0);
        inputDelimiter = dsResolver.inputDelimiter(inputName);

        outputName = opResolver.positionalOutput(0);
        outputDelimiter = dsResolver.outputDelimiter(outputName);

        Map<String, Integer> inputColumns = dsResolver.inputColumns(inputName);

        List<Integer> out = new ArrayList<>();
        String[] outColumns = dsResolver.outputColumns(outputName);
        if (outColumns != null) {
            for (String outCol : outColumns) {
                out.add(inputColumns.get(outCol));
            }

            outputColumns = out.stream().mapToInt(i -> i).toArray();
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        final int[] _outputColumns = outputColumns;
        final char _outputDelimiter = outputDelimiter;
        final char _inputDelimiter = inputDelimiter;

        JavaRDD<Text> output = ((JavaPairRDD<Object, Object>) input.get(inputName)).mapPartitions(it -> {
            List<Text> ret = new ArrayList<>();

            CSVParser parser = new CSVParserBuilder().withSeparator(_inputDelimiter).build();

            while (it.hasNext()) {
                Tuple2<Object, Object> v = it.next();

                if (_outputColumns != null) {
                    String l = v._2 instanceof String ? (String) v._2 : String.valueOf(v._2);

                    String[] ll = parser.parseLine(l);

                    String[] acc = new String[_outputColumns.length];

                    int i = 0;
                    for (Integer col : _outputColumns) {
                        acc[i++] = ll[col];
                    }

                    StringWriter buffer = new StringWriter();

                    CSVWriter writer = new CSVWriter(buffer, _outputDelimiter, CSVWriter.DEFAULT_QUOTE_CHARACTER, CSVWriter.DEFAULT_ESCAPE_CHARACTER, "");
                    writer.writeNext(acc, false);
                    writer.close();

                    ret.add(new Text(buffer.toString()));
                } else {
                    ret.add(new Text(v._1 + String.valueOf(_outputDelimiter) + v._2));
                }
            }

            return ret.iterator();
        });

        return Collections.singletonMap(outputName, output);
    }
}
