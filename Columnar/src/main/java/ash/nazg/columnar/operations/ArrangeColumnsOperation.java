/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.columnar.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.Description;
import ash.nazg.config.tdl.StreamType;
import ash.nazg.config.tdl.TaskDescriptionLanguage;
import ash.nazg.spark.Operation;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVWriter;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unused")
public class ArrangeColumnsOperation extends Operation {
    public static final String VERB = "arrangeColumns";

    private String inputName;
    private Character inputDelimiter;
    private String outputName;
    private Character outputDelimiter;
    private int[] outputColumns;

    @Override
    @Description("This operation rearranges the order of input CSV columns, optionally omitting unneeded in the output")
    public String verb() {
        return VERB;
    }

    @Override
    public TaskDescriptionLanguage.Operation description() {
        return new TaskDescriptionLanguage.Operation(verb(),
                null,

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new StreamType[]{StreamType.CSV},
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
        for (String outCol : outColumns) {
            out.add(inputColumns.get(outCol));
        }

        outputColumns = out.stream().mapToInt(i -> i).toArray();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        JavaRDD<Object> inp = (JavaRDD<Object>) input.get(inputName);

        final int[] _outputColumns = outputColumns;
        final char _outputDelimiter = outputDelimiter;
        final char _inputDelimiter = inputDelimiter;

        JavaRDD<Text> out = inp.mapPartitions(it -> {
            CSVParser parser = new CSVParserBuilder().withSeparator(_inputDelimiter).build();

            List<Text> ret = new ArrayList<>();
            while (it.hasNext()) {
                Object v = it.next();
                String l = v instanceof String ? (String) v : String.valueOf(v);

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
            }

            return ret.iterator();
        });

        return Collections.singletonMap(outputName, out);
    }
}
