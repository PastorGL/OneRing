/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.columnar.operations;

import ash.nazg.commons.DigestUtils;
import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.config.tdl.StreamType;
import ash.nazg.config.tdl.metadata.OperationMeta;
import ash.nazg.config.tdl.metadata.PositionalStreamsMetaBuilder;
import ash.nazg.spark.Operation;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVWriter;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import scala.Tuple2;
import scala.Tuple3;

import javax.xml.bind.DatatypeConverter;
import java.io.StringWriter;
import java.security.MessageDigest;
import java.security.Provider;
import java.security.Security;
import java.util.*;

@SuppressWarnings("unused")
public class DigestOperation extends Operation {
    private static final Map<String, String> KNOWN_COLUMNS = new HashMap<>();

    private Tuple3<String, String, int[]>[] outputCols;
    private String outputName;
    private String inputName;
    private char inputDelimiter;
    private char outputDelimiter;

    static {
        String digest = MessageDigest.class.getSimpleName();

        Provider[] providers = Security.getProviders();
        for (Tuple2<String, String> digestAlgo : DigestUtils.DIGEST_ALGOS) {
            String provider = digestAlgo._1;
            String algorithm = digestAlgo._2;

            KNOWN_COLUMNS.put("_" + provider + "_" + algorithm + "_*", "Provider " + provider + " algorithm " + algorithm);
        }
    }

    @Override
    public OperationMeta meta() {
        return new OperationMeta("digest", "This operation calculates cryptographic digest(s) for given input column(s)," +
                " by any algorithm provided by Java platform. Possible generated column list is dynamic," +
                " while each column name follows the convention of _PROVIDER_ALGORITHM_source.column." +
                " Default PROVIDER is SUN, and ALGORITHMs are MD2, MD5, SHA, SHA-224, SHA-256, SHA-384 and SHA-512",

                new PositionalStreamsMetaBuilder()
                        .ds("Input CSV to calculate digests for columns",
                                new StreamType[]{StreamType.CSV}, true
                        )
                        .build(),

                null,

                new PositionalStreamsMetaBuilder()
                        .ds("Output CSV with additional digest columns",
                                new StreamType[]{StreamType.CSV}, true)
                        .genCol(KNOWN_COLUMNS)
                        .build()
        );
    }

    @Override
    @SuppressWarnings("unchecked")
    public void configure() throws InvalidConfigValueException {
        inputName = opResolver.positionalInput(0);
        inputDelimiter = dsResolver.inputDelimiter(inputName);
        outputName = opResolver.positionalOutput(0);
        outputDelimiter = dsResolver.outputDelimiter(outputName);

        Map<String, Integer> inputColumns = dsResolver.inputColumns(inputName);
        String[] outputColumns = dsResolver.outputColumns(outputName);

        outputCols = new Tuple3[outputColumns.length];

        checkOutputColumns:
        for (int i = 0; i < outputColumns.length; i++) {
            String outputCol = outputColumns[i];

            if (inputColumns.containsKey(outputCol)) {
                outputCols[i] = new Tuple3<>(null, null, new int[]{inputColumns.get(outputCol)});
            } else {
                for (String prefix : KNOWN_COLUMNS.keySet()) {
                    if (outputCol.startsWith(prefix.substring(0, prefix.length() - 2))) {
                        String[] columnRef = outputCol.split("_", 4);

                        int[] colNo;
                        if (columnRef[3].startsWith("{")) {
                            colNo = Arrays.stream(columnRef[3].substring(1, columnRef[3].length() - 1).split(","))
                                    .map(inputColumns::get).mapToInt(Integer::intValue).toArray();
                        } else {
                            colNo = new int[]{inputColumns.get(columnRef[3])};
                        }

                        outputCols[i] = new Tuple3<>(columnRef[1], columnRef[2], colNo);
                        continue checkOutputColumns;
                    }
                }

                throw new InvalidConfigValueException("Output column '" + outputCol + "' doesn't reference input nor can be generated in the operation '" + name + "'");
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        final char _inputDelimiter = inputDelimiter;
        final Tuple3<String, String, int[]>[] _outputCols = outputCols;
        final char _outputDelimiter = outputDelimiter;

        JavaRDD<Text> output = ((JavaRDD<Object>) input.get(inputName))
                .mapPartitions(it -> {
                            List<Text> ret = new ArrayList<>();

                            Map<String, MessageDigest> digests = new HashMap<>();
                            for (Tuple3<String, String, int[]> outputCol : _outputCols) {
                                String provider = outputCol._1();
                                String algorithm = outputCol._2();

                                if (provider != null) {
                                    digests.put("_" + provider + "_" + algorithm + "_", MessageDigest.getInstance(algorithm, provider));
                                }
                            }

                            CSVParser parser = new CSVParserBuilder().withSeparator(_inputDelimiter)
                                    .build();
                            while (it.hasNext()) {
                                Object o = it.next();
                                String l = o instanceof String ? (String) o : String.valueOf(o);
                                String[] row = parser.parseLine(l);

                                String[] acc = new String[_outputCols.length];
                                for (int i = 0; i < _outputCols.length; i++) {
                                    String provider = _outputCols[i]._1();

                                    if (provider == null) {
                                        acc[i] = row[_outputCols[i]._3()[0]];
                                    } else {
                                        MessageDigest md = digests.get("_" + provider + "_" + _outputCols[i]._2() + "_");

                                        int[] refs = _outputCols[i]._3();
                                        for (int j = 0, length = refs.length; j < length; j++) {
                                            int col = refs[j];
                                            if (j > 0) {
                                                md.update((byte) 0);
                                            }
                                            md.update(row[col].getBytes());
                                        }

                                        acc[i] = DatatypeConverter.printHexBinary(md.digest());
                                    }
                                }

                                StringWriter buffer = new StringWriter();
                                CSVWriter writer = new CSVWriter(buffer, _outputDelimiter,
                                        CSVWriter.DEFAULT_QUOTE_CHARACTER, CSVWriter.DEFAULT_ESCAPE_CHARACTER, "");

                                writer.writeNext(acc, false);
                                writer.close();

                                ret.add(new Text(buffer.toString()));
                            }

                            return ret.iterator();
                        }
                );

        return Collections.singletonMap(outputName, output);
    }
}
