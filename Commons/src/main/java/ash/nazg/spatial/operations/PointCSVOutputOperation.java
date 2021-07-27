/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.spatial.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.StreamType;
import ash.nazg.config.tdl.metadata.OperationMeta;
import ash.nazg.config.tdl.metadata.PositionalStreamsMetaBuilder;
import ash.nazg.spark.Operation;
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
    private String inputName;

    private String outputName;
    private char outputDelimiter;
    private List<String> outputColumns;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("pointCsvOutput", "Take a Point RDD and produce a CSV file",

                new PositionalStreamsMetaBuilder()
                        .ds("Point RDD",
                                new StreamType[]{StreamType.Point}
                        )
                        .build(),

                null,

                new PositionalStreamsMetaBuilder()
                        .ds("CSV RDD with Point attributes",
                                new StreamType[]{StreamType.CSV}, true
                        )
                        .build()
        );
    }

    @Override
    public void configure() throws InvalidConfigValueException {
        inputName = opResolver.positionalInput(0);
        outputName = opResolver.positionalOutput(0);

        outputDelimiter = dsResolver.outputDelimiter(outputName);
        String[] outputCols = dsResolver.outputColumns(outputName);
        outputColumns = (outputCols == null) ? Collections.emptyList() : Arrays.stream(outputCols)
                .map(c -> c.replaceFirst("^" + inputName + "\\.", ""))
                .collect(Collectors.toList());
    }

    @SuppressWarnings("rawtypes")
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
                            out[i++] = String.valueOf(t.get(new Text(column)));
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
