/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.spatial.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.Description;
import ash.nazg.config.tdl.TaskDescriptionLanguage;
import ash.nazg.spark.Operation;
import ash.nazg.config.OperationConfig;
import com.opencsv.CSVWriter;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.locationtech.jts.geom.Point;

import java.io.StringWriter;
import java.util.*;
import java.util.stream.Collectors;

@SuppressWarnings("unused")
public class PointCSVOutputOperation extends Operation {
    public static final String VERB = "pointCsvOutput";

    private String inputName;
    private String outputName;
    private char outputDelimiter;
    private List<String> outputColumns;

    @Override
    @Description("Take a Point RDD and produce a CSV file")
    public String verb() {
        return VERB;
    }

    @Override
    public TaskDescriptionLanguage.Operation description() {
        return new TaskDescriptionLanguage.Operation(verb(),
                null,

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.Point},
                                false
                        )
                ),

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.CSV},
                                true
                        )
                )
        );
    }

    @Override
    public void setConfig(OperationConfig propertiesConfig) throws InvalidConfigValueException {
        super.setConfig(propertiesConfig);

        inputName = describedProps.inputs.get(0);
        outputName = describedProps.outputs.get(0);

        outputDelimiter = dataStreamsProps.outputDelimiter(outputName);
        outputColumns = Arrays.stream(dataStreamsProps.outputColumns.get(outputName))
                .map(c -> c.replaceFirst("^" + inputName + "\\.", ""))
                .collect(Collectors.toList());
    }

    @Override
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        final char _outputDelimiter = outputDelimiter;
        final List<String> _outputColumns = outputColumns;

        JavaRDD<Text> output = ((JavaRDD<Point>) input.get(inputName))
                .mapPartitions(it -> {
                    List<Text> ret = new ArrayList<>();

                    while (it.hasNext()) {
                        MapWritable t = (MapWritable) it.next().getUserData();

                        String[] out = new String[_outputColumns.size()];

                        int i = 0;
                        for (String column : _outputColumns) {
                            out[i++] = t.get(new Text(column)).toString();
                        }

                        StringWriter buffer = new StringWriter();
                        CSVWriter writer = new CSVWriter(buffer, _outputDelimiter, CSVWriter.DEFAULT_QUOTE_CHARACTER,
                                CSVWriter.DEFAULT_ESCAPE_CHARACTER, "");
                        writer.writeNext(out, false);
                        writer.close();

                        ret.add(new Text(buffer.toString()));
                    }

                    return ret.iterator();
                });

        return Collections.singletonMap(outputName, output);
    }
}
