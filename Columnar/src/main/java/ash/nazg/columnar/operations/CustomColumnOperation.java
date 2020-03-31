/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.columnar.operations;

import ash.nazg.spark.Operation;
import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.OperationConfig;
import ash.nazg.config.tdl.Description;
import ash.nazg.config.tdl.TaskDescriptionLanguage;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVWriter;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;

import java.io.StringWriter;
import java.util.*;

@SuppressWarnings("unused")
public class CustomColumnOperation extends Operation {
    @Description("Fixed value of a custom column (opaque, not parsed in any way)")
    public static final String OP_CUSTOM_COLUMN_VALUE = "custom.column.value";
    @Description("Position to insert a column. Counts from 0 onwards from the beginning of a row, or from the end if < 0 (-1 is last and so on)")
    public static final String OP_CUSTOM_COLUMN_INDEX = "custom.column.index";
    @Description("By default, add to the end of a row")
    public static final Integer DEF_CUSTOM_COLUMN_INDEX = -1;

    public static final String VERB = "customColumn";

    private String inputName;
    private char inputDelimiter;
    private String outputName;

    private String columnValue;
    private Integer columnIndex;

    @Override
    @Description("Insert a custom column in the input CSV at a given index")
    public String verb() {
        return VERB;
    }

    @Override
    public TaskDescriptionLanguage.Operation description() {
        return new TaskDescriptionLanguage.Operation(verb(),
                new TaskDescriptionLanguage.DefBase[]{
                        new TaskDescriptionLanguage.Definition(OP_CUSTOM_COLUMN_VALUE),
                        new TaskDescriptionLanguage.Definition(OP_CUSTOM_COLUMN_INDEX, Integer.class, DEF_CUSTOM_COLUMN_INDEX),
                },

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.CSV},
                                false
                        )
                ),

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new TaskDescriptionLanguage.StreamType[]{TaskDescriptionLanguage.StreamType.Passthru},
                                false
                        )
                )
        );
    }

    @Override
    public void configure(Properties properties, Properties variables) throws InvalidConfigValueException {
        super.configure(properties, variables);

        inputName = describedProps.inputs.get(0);
        inputDelimiter = dataStreamsProps.inputDelimiter(inputName);
        outputName = describedProps.outputs.get(0);

        columnValue = describedProps.defs.getTyped(OP_CUSTOM_COLUMN_VALUE);
        columnIndex = describedProps.defs.getTyped(OP_CUSTOM_COLUMN_INDEX);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        final char _inputDelimiter = inputDelimiter;
        final String _columnValue = columnValue;
        final Integer _columnIndex = columnIndex;

        JavaRDD<Text> out = ((JavaRDD<Object>) input.get(inputName))
                .mapPartitions(it -> {
                    CSVParser parser = new CSVParserBuilder().withSeparator(_inputDelimiter)
                            .build();

                    List<Text> ret = new ArrayList<>();
                    while (it.hasNext()) {
                        Object o = it.next();
                        String l = o instanceof String ? (String) o : String.valueOf(o);
                        String[] row = parser.parseLine(l);
                        String[] newRow = new String[row.length + 1];
                        int columnIndex = _columnIndex < 0
                                ? row.length + 1 + _columnIndex
                                : _columnIndex;

                        System.arraycopy(row, 0, newRow, 0, columnIndex);
                        newRow[columnIndex] = _columnValue;
                        System.arraycopy(row, columnIndex, newRow, columnIndex + 1, row.length - columnIndex);

                        StringWriter buffer = new StringWriter();
                        CSVWriter writer = new CSVWriter(buffer, _inputDelimiter,
                                CSVWriter.DEFAULT_QUOTE_CHARACTER, CSVWriter.DEFAULT_ESCAPE_CHARACTER, "");

                        writer.writeNext(newRow, false);
                        writer.close();

                        ret.add(new Text(buffer.toString()));
                    }
                    return ret.iterator();
                });

        return Collections.singletonMap(outputName, out);
    }
}
