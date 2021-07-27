/**
 * Copyright (C) 2020 Locomizer team and Contributors
 * This project uses New BSD license with do no evil clause. For full text, check the LICENSE file in the root directory.
 */
package ash.nazg.geohashing.operations;

import ash.nazg.config.InvalidConfigValueException;
import ash.nazg.geohashing.functions.HasherFunction;
import ash.nazg.spark.Operation;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVWriter;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.sparkproject.guava.primitives.Ints;
import scala.Tuple2;
import scala.Tuple3;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public abstract class GeohashingOperation extends Operation {
    public static final String DS_LAT_COLUMN = "lat.column";
    public static final String DS_LON_COLUMN = "lon.column";
    public static final String OP_HASH_LEVEL = "hash.level";
    public static final String GEN_HASH = "_hash";

    protected Integer level;
    private String inputName;
    private char inputDelimiter;
    private String outputName;
    private char outputDelimiter;
    private int[] outputColumns;
    private Integer latColumn;
    private Integer lonColumn;
    private HasherFunction hasher;

    @Override
    public void configure() throws InvalidConfigValueException {
        inputName = opResolver.positionalInput(0);
        inputDelimiter = dsResolver.inputDelimiter(inputName);
        outputName = opResolver.positionalOutput(0);
        outputDelimiter = dsResolver.outputDelimiter(outputName);

        Map<String, Integer> inputColumns = dsResolver.inputColumns(inputName);
        String prop;

        prop = opResolver.definition(DS_LAT_COLUMN);
        latColumn = inputColumns.get(prop);

        prop = opResolver.definition(DS_LON_COLUMN);
        lonColumn = inputColumns.get(prop);

        List<Integer> out = new ArrayList<>();
        String[] outColumns = dsResolver.outputColumns(outputName);
        for (String outCol : outColumns) {
            if (inputColumns.containsKey(outCol)) {
                out.add(inputColumns.get(outCol));
            }
            if (GEN_HASH.equalsIgnoreCase(outCol)) {
                out.add(-1);
            }
        }

        outputColumns = Ints.toArray(out);

        level = opResolver.definition(OP_HASH_LEVEL);

        if (level < getMinLevel() || level > getMaxLevel()) {
            throw new InvalidConfigValueException("Geohash level must fall into interval '" + getMinLevel() + "'..'" + getMaxLevel() + "' but is '" + level + "' in the operation '" + name + "'");
        }

        try {
            hasher = getHasher();
        } catch (Exception e) {
            throw new InvalidConfigValueException("Geohasher can't initialize with an exception for the operation '" + name + "'", e);
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) {
        JavaRDD<Object> inp = (JavaRDD<Object>) input.get(inputName);

        final int _latColumn = latColumn;
        final int _lonColumn = lonColumn;
        final int[] _outputColumns = outputColumns;
        final char _outputDelimiter = outputDelimiter;
        final char _inputDelimiter = inputDelimiter;

        final HasherFunction _hasher = hasher;

        JavaRDD out = inp
                .mapPartitions(it -> {
                    List<Tuple3<Double, Double, Text>> ret = new ArrayList<>();

                    CSVParser parser = new CSVParserBuilder().withSeparator(_inputDelimiter).build();

                    while (it.hasNext()) {
                        Object v = it.next();
                        String l = v instanceof String ? (String) v : String.valueOf(v);

                        String[] ll = parser.parseLine(l);
                        Double lat = new Double(ll[_latColumn]);
                        Double lon = new Double(ll[_lonColumn]);

                        ret.add(new Tuple3<>(lat, lon, new Text(l)));
                    }

                    return ret.iterator();
                })
                .mapPartitions(_hasher)
                .mapPartitions(it -> {
                    List<Text> ret = new ArrayList<>();

                    CSVParser parser = new CSVParserBuilder().withSeparator(_inputDelimiter).build();

                    while (it.hasNext()) {
                        Tuple2<Text, Text> v = it.next();

                        String hash = String.valueOf(v._1);

                        String l = String.valueOf(v._2);
                        String[] ll = parser.parseLine(l);

                        String[] acc = new String[_outputColumns.length];
                        int i = 0;
                        for (Integer col : _outputColumns) {
                            acc[i++] = (col >= 0) ? ll[col] : hash;
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

    protected abstract int getMinLevel();

    protected abstract int getMaxLevel();

    protected abstract Integer getDefaultLevel();

    protected abstract HasherFunction getHasher();
}
