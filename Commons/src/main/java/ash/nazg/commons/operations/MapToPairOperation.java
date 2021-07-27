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
import com.opencsv.CSVWriter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import scala.Tuple2;

import javax.xml.bind.DatatypeConverter;
import java.io.StringWriter;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unused")
public class MapToPairOperation extends Operation {
    public static final String OP_KEY_LENGTH = "key.length";
    public static final String OP_KEY_COLUMNS = "key.columns";
    public static final String OP_KEY_DIGEST = "key.digest";
    public static final String OP_VALUE_COLUMNS = "value.columns";

    private String inputName;
    private char inputDelimiter;

    private String outputName;
    private char outputDelimiter;

    private int[] keyColumns;
    private Integer keyLength;
    private int[] valueColumns;

    private String keyProvider;
    private String keyAlgorithm;

    @Override
    public OperationMeta meta() {
        return new OperationMeta("mapToPair", "Take a CSV RDD and transform it to PairRDD using selected columns to build a key," +
                " optionally limiting key size and/or turn it into a cryptographic digest",

                new PositionalStreamsMetaBuilder()
                        .ds("A CSV RDD to turn into a Pair RDD",
                                new StreamType[]{StreamType.CSV}, true
                        )
                        .build(),

                new DefinitionMetaBuilder()
                        .def(OP_KEY_COLUMNS, "To form a key, selected column values (in order, specified here)" +
                                " are glued together with an output delimiter", String[].class)
                        .def(OP_VALUE_COLUMNS, "To form a value, selected column values (in order, specified here)" +
                                        " are glued together with an output delimiter", String[].class,
                                null, "If not set (and by default) use entire source line as a value as it is")
                        .def(OP_KEY_LENGTH, "Key size limit to set number of characters. This limit is NOT applied to a digested key" +
                                        " which is always of size of digest's hexadecimal representation", Integer.class,
                                "-1", "If needed, limit key length by the set number of characters. By default, don't")
                        .def(OP_KEY_DIGEST, "If set to _PROVIDER_ALGORITHM_, create a cryptographic digest of the key." +
                                " For the list of available providers and algorithms, refer to operation 'digest'. This operation" +
                                " uses same method, and is compatible with its implementation", null, "By default, don't digest the key")
                        .build(),

                new PositionalStreamsMetaBuilder()
                        .ds("A Pair RDD",
                                new StreamType[]{StreamType.KeyValue}, true
                        )
                        .build()
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

        String keyDigest = opResolver.definition(OP_KEY_DIGEST);
        if (!StringUtils.isEmpty(keyDigest)) {
            String[] digest = keyDigest.split("_", 4);
            keyProvider = digest[1];
            keyAlgorithm = digest[2];
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        final char _inputDelimiter = inputDelimiter;
        final int[] _keyColumns = keyColumns;
        final int _keyLength = keyLength;
        final int[] _valueColumns = valueColumns;
        final char _outputDelimiter = outputDelimiter;
        final String _provider = keyProvider;
        final String _algorithm = keyAlgorithm;

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
                    List<Tuple2<Text, Text>> ret = new ArrayList<>();

                    MessageDigest md = null;
                    if (_provider != null) {
                        md = MessageDigest.getInstance(_algorithm, _provider);
                    }

                    CSVParser parser = new CSVParserBuilder().withSeparator(_inputDelimiter).build();
                    while (it.hasNext()) {
                        Object v = it.next();
                        String l = v instanceof String ? (String) v : String.valueOf(v);

                        String[] line = parser.parseLine(l);

                        StringWriter buffer = new StringWriter();
                        CSVWriter writer = new CSVWriter(buffer, _outputDelimiter, CSVWriter.DEFAULT_QUOTE_CHARACTER,
                                CSVWriter.DEFAULT_ESCAPE_CHARACTER, "");
                        String[] columns;
                        String key;

                        if (md != null) {
                            for (int j = 0, length = _keyColumns.length; j < length; j++) {
                                int col = _keyColumns[j];
                                if (j > 0) {
                                    md.update((byte) 0);
                                }
                                md.update(line[col].getBytes());
                            }

                            key = DatatypeConverter.printHexBinary(md.digest());
                        } else {
                            columns = new String[_keyColumns.length];
                            for (int i = 0; i < _keyColumns.length; i++) {
                                columns[i] = line[_keyColumns[i]];
                            }

                            writer.writeNext(columns, false);
                            writer.close();

                            key = buffer.toString();
                            int length = key.length();
                            if (_keyLength > 0) {
                                if (length > _keyLength) {
                                    length = _keyLength;
                                }

                                key = key.substring(0, length);
                            }
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
