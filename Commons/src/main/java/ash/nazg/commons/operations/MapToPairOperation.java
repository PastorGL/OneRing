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
public class MapToPairOperation extends Operation {
    @Description("Key size limit to set number of characters")
    public static final String OP_KEY_LENGTH = "key.length";
    @Description("To form a key, selected column values (in order, specified here)" +
            " are glued together with an output delimiter")
    public static final String OP_KEY_COLUMNS = "key.columns";
    @Description("To form a value, selected column values (in order, specified here)" +
            " are glued together with an output delimiter")
    public static final String OP_VALUE_COLUMNS = "value.columns";
    @Description("If needed, limit key length by the set number of characters. By default, don't")
    public static final Integer DEF_KEY_LENGTH = -1;
    @Description("If not set (and by default) use entire source line as a value as it is")
    public static final String[] DEF_VALUE_COLUMNS = null;

    public static final String VERB = "mapToPair";

    private String inputName;
    private int[] keyColumns;
    private int[] valueColumns;
    private char inputDelimiter;
    private char outputDelimiter;
    private String outputName;
    private Integer keyLength;

    @Override
    @Description("Take a CSV RDD and transform it to PairRDD using selected columns to build a key," +
            " optionally limiting key size")
    public String verb() {
        return VERB;
    }

    @Override
    public TaskDescriptionLanguage.Operation description() {
        return new TaskDescriptionLanguage.Operation(verb(),
                new TaskDescriptionLanguage.DefBase[]{
                        new TaskDescriptionLanguage.Definition(OP_KEY_COLUMNS, String[].class),
                        new TaskDescriptionLanguage.Definition(OP_VALUE_COLUMNS, String[].class, DEF_VALUE_COLUMNS),
                        new TaskDescriptionLanguage.Definition(OP_KEY_LENGTH, Integer.class, DEF_KEY_LENGTH),
                },

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new StreamType[]{StreamType.CSV},
                                true
                        )
                ),

                new TaskDescriptionLanguage.OpStreams(
                        new TaskDescriptionLanguage.DataStream(
                                new StreamType[]{StreamType.KeyValue},
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

        keyLength = opResolver.definition(OP_KEY_LENGTH);

        String[] columns = opResolver.definition(OP_KEY_COLUMNS);
        keyColumns = new int[columns.length];
        int i = 0;
        for (String kc : columns) {
            keyColumns[i] = inputColumns.get(kc);
            i++;
        }

        columns = opResolver.definition(OP_VALUE_COLUMNS);
        if (columns != null) {
            valueColumns = new int[columns.length];
            i = 0;
            for (String kc : columns) {
                valueColumns[i] = inputColumns.get(kc);
                i++;
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        final char _inputDelimiter = inputDelimiter;
        final int[] _keyColumns = keyColumns;
        int _keyLength = keyLength;
        final int[] _valueColumns = valueColumns;
        char _outputDelimiter = outputDelimiter;

        JavaRDDLike inp = input.get(inputName);
        JavaRDD<Object> rdd = null;
        if (inp instanceof JavaRDD) {
            rdd = (JavaRDD) inp;
        }
        if (inp instanceof JavaPairRDD) {
            rdd = ((JavaPairRDD) inp).values();
        }

        JavaPairRDD<Text, Text> out = rdd
                .mapPartitionsToPair(it -> {
                    CSVParser parser = new CSVParserBuilder().withSeparator(_inputDelimiter).build();

                    List<Tuple2<Text, Text>> ret = new ArrayList<>();
                    while (it.hasNext()) {
                        Object v = it.next();
                        String l = v instanceof String ? (String) v : String.valueOf(v);

                        String[] line = parser.parseLine(l);

                        StringWriter buffer = new StringWriter();
                        CSVWriter writer = new CSVWriter(buffer, _outputDelimiter, CSVWriter.DEFAULT_QUOTE_CHARACTER,
                                CSVWriter.DEFAULT_ESCAPE_CHARACTER, "");

                        String[] columns = new String[_keyColumns.length];
                        for (int i = 0; i < _keyColumns.length; i++) {
                            columns[i] = line[_keyColumns[i]];
                        }

                        writer.writeNext(columns, false);
                        writer.close();

                        String key = buffer.toString();
                        int length = key.length();
                        if (_keyLength > 0) {
                            if (length > _keyLength) {
                                length = _keyLength;
                            }

                            key = key.substring(0, length);
                        }

                        String value = l;
                        if (_valueColumns != null) {
                            buffer = new StringWriter();
                            writer = new CSVWriter(buffer, _outputDelimiter, CSVWriter.DEFAULT_QUOTE_CHARACTER,
                                    CSVWriter.DEFAULT_ESCAPE_CHARACTER, "");

                            columns = new String[_valueColumns.length];
                            for (int i = 0; i < _valueColumns.length; i++) {
                                columns[i] = line[_valueColumns[i]];
                            }

                            writer.writeNext(columns, false);
                            writer.close();

                            value = buffer.toString();
                        }

                        ret.add(new Tuple2<>(new Text(key), new Text(value)));
                    }

                    return ret.iterator();
                });

        return Collections.singletonMap(outputName, out);
    }
}
